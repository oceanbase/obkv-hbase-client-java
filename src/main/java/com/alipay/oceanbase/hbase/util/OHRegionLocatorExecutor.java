package com.alipay.oceanbase.hbase.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alipay.oceanbase.hbase.execute.AbstractObTableMetaExecutor;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.constant.Constants;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.location.model.TableEntry;
import com.alipay.oceanbase.rpc.meta.ObTableMetaRequest;
import com.alipay.oceanbase.rpc.meta.ObTableMetaResponse;
import com.alipay.oceanbase.rpc.meta.ObTableRpcMetaType;
import org.apache.hadoop.hbase.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OHRegionLocatorExecutor extends AbstractObTableMetaExecutor<OHRegionLocator> {
    private final String tableName;
    private final ObTableClient client;

    OHRegionLocatorExecutor(String tableName, ObTableClient client) {
        this.tableName = tableName;
        this.client = client;
    }

    @Override
    public ObTableRpcMetaType getMetaType() {
        return ObTableRpcMetaType.HTABLE_REGION_LOCATOR;
    }

    /**
     * Parses the response and creates a region locator
     * @param response response from the server
     * @return OHRegionLocator
     * @throws IOException if failed to parse the response
     */
    @Override
    public OHRegionLocator parse(ObTableMetaResponse response) throws IOException {
        try {
            final String jsonData = response.getData();
            final JSONObject jsonMap = Optional.<JSONObject>ofNullable(JSON.parseObject(jsonData))
                    .orElseThrow(() -> new IOException("jsonMap is null"));
            /*
                  {
                    "table_id_dict": [1001, 1002],
                    "replica_dict": [
                      ["127.0.0.1", 2881],
                      ["127.0.0.2", 2882],
                      ["127.0.0.3", 2883]
                    ],
                    "partitions": [
                      // 表1001的3个分区，每个分区3副本
                      [0, 50001, "rowkey_1", 0, 1], // leader
                      [0, 50001, "rowkey_1", 1, 0], // follower
                      [0, 50001, "rowkey_1", 2, 0], // follower
                      [0, 50002, "rowkey_2", 0, 1],
                      [0, 50002, "rowkey_2", 1, 0],
                      [0, 50002, "rowkey_2", 2, 0],
                      [0, 50003, "rowkey_3", 0, 1],
                      [0, 50003, "rowkey_3", 1, 0],
                      [0, 50003, "rowkey_3", 2, 0],
                
                      // 表1002的3个分区，每个分区3副本
                      [1, 50004, "rowkey_1", 0, 1],
                      [1, 50004, "rowkey_1", 1, 0],
                      [1, 50004, "rowkey_1", 2, 0],
                      [1, 50005, "rowkey_2", 0, 1],
                      [1, 50005, "rowkey_2", 1, 0],
                      [1, 50005, "rowkey_2", 2, 0],
                      [1, 50006, "rowkey_3", 0, 1],
                      [1, 50006, "rowkey_3", 1, 0],
                      [1, 50006, "rowkey_3", 2, 0]
                    ]
                  }
             */

            final List<Object> partitions = Optional.<List<Object>>ofNullable(jsonMap.getJSONArray("partitions"))
                    .orElseThrow(() -> new IOException("partitions is null"));

            final List<Object> tableIdDict = Optional.<List<Object>>ofNullable(jsonMap.getJSONArray("table_id_dict"))
                    .orElseThrow(() -> new IOException("tableIdDict is null"));
            final List<Object> replicaDict = Optional.<List<Object>>ofNullable(jsonMap.getJSONArray("replica_dict"))
                    .orElseThrow(() -> new IOException("replicaDict is null"));

            final boolean isHashLikePartition = partitions.stream()
                    .map(obj -> (List<Object>) obj)
                    .filter(partition -> {
                        if (partition.size() <= 3) {
                            throw new IllegalArgumentException("partition size is not 3");
                        }
                        return true;
                    })
                    .allMatch(partition -> {
                        final byte[] highBound = partition.get(2).toString().getBytes();
                        return Arrays.equals(highBound, Constants.EMPTY_STRING.getBytes());
                    });
            return isHashLikePartition ?
                    createHashPartitionLocator(tableIdDict, replicaDict, partitions) :
                    createRangePartitionLocator(tableIdDict, replicaDict, partitions);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid partition data: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a region locator for range partitions
     * @param tableIdDict table ID dictionary
     * @param replicaDict replica dictionary
     * @param partitions list of partition data
     * @return OHRegionLocator for range partitions
     */
    private OHRegionLocator createRangePartitionLocator(
            final List<Object> tableIdDict,
            final List<Object> replicaDict,
            final List<Object> partitions) {
        if ((partitions.size() % tableIdDict.size()) != 0) {
            throw new ObTableUnexpectedException(
                    "The number of partitions should be an integer multiple of the number of tables");
        }
        // the size of partitions the multiple of the number of zones, the number of tablets and the number of tables
        final int regionCount = partitions.size() / tableIdDict.size(); // get tablet boundaries of leaders and followers
        final List<byte[]> startKeysList = new ArrayList<>();
        final List<byte[]> endKeysList = new ArrayList<>();

        for (int i = 0; i < regionCount; ++i) {
            boolean isLeader = ((int) ((List<Object>) partitions.get(i)).get(4) == 1);
            if (!isLeader) { // only record leader's boundary
                continue;
            }
            if (i == 0) {
                startKeysList.add(HConstants.EMPTY_BYTE_ARRAY);
                endKeysList.add(((List<Object>) partitions.get(i)).get(2).toString().getBytes());
            } else if (i == regionCount - 1) {
                startKeysList.add(((List<Object>) partitions.get(i - 1)).get(2).toString().getBytes());
                endKeysList.add(HConstants.EMPTY_BYTE_ARRAY);
            } else {
                startKeysList.add(((List<Object>) partitions.get(i - 1)).get(2).toString().getBytes());
                endKeysList.add(((List<Object>) partitions.get(i)).get(2).toString().getBytes());
            }
        }
        final byte[][] startKeys = startKeysList.toArray(new byte[0][]);
        final byte[][] endKeys = endKeysList.toArray(new byte[0][]);
        // Create region locations for all regions
        final List<HRegionLocation> regionLocations = IntStream.range(0, regionCount)
                .mapToObj(i -> {
                    final List<Object> partition = (List<Object>) partitions.get(Math.min(i, regionCount - 1));
                    final int replicationIdx = (int) partition.get(3);
                    final List<Object> hostInfo = (List<Object>) replicaDict.get(replicationIdx);

                    final ServerName serverName = ServerName.valueOf(
                            hostInfo.get(0).toString(),
                            (int) hostInfo.get(1),
                            i
                    );
                    final HRegionInfo regionInfo = new HRegionInfo(
                            TableName.valueOf(tableName),
                            startKeys[i],
                            endKeys[i]
                    );
                    return new HRegionLocation(regionInfo, serverName, i);
                })
                .collect(Collectors.toList());

        return new OHRegionLocator(startKeys, endKeys, regionLocations, TableName.valueOf(tableName), client);
    }

    /**
     * Creates a region locator for hash partitions
     * @param tableIdDict table ID dictionary
     * @param replicaDict replica dictionary
     * @param partitions list of partition data
     * @return OHRegionLocator for hash partitions
     */
    private OHRegionLocator createHashPartitionLocator(
            final List<Object> tableIdDict,
            final List<Object> replicaDict,
            final List<Object> partitions) {

        final byte[][] startKeys = new byte[1][];
        final byte[][] endKeys = new byte[1][];
        startKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
        endKeys[0] = HConstants.EMPTY_BYTE_ARRAY;

        final List<HRegionLocation> regionLocations = IntStream.range(0, partitions.size())
                .mapToObj(i -> {
                    final List<Object> partition = (List<Object>) partitions.get(i);
                    final int replicationIdx = (int) partition.get(3);
                    final List<Object> hostInfo = (List<Object>) replicaDict.get(replicationIdx);

                    final ServerName serverName = ServerName.valueOf(
                            hostInfo.get(0).toString(),
                            (int) hostInfo.get(1),
                            i
                    );
                    final HRegionInfo regionInfo = new HRegionInfo(
                            TableName.valueOf(tableName),
                            startKeys[0],
                            endKeys[0]
                    );
                    return new HRegionLocation(regionInfo, serverName, i);
                })
                .collect(Collectors.toList());

        return new OHRegionLocator(startKeys, endKeys, regionLocations, TableName.valueOf(tableName), client);
    }

    public OHRegionLocator getRegionLocator(final String tableName) throws IOException {
        final ObTableMetaRequest request = new ObTableMetaRequest();
        request.setMetaType(getMetaType());
        final Map<String, String> requestData = new HashMap<>();
        requestData.put("table_name", tableName);

        final String jsonData = JSON.toJSONString(requestData);
        request.setData(jsonData);

        return execute(client, request);
    }
}
