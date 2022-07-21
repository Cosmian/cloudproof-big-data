package com.cosmian.cloudproof_demo;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class RecordUid {
    public final String filename;

    public final long mark;

    private final byte[] fileUid;

    public RecordUid(String filename, long mark) {
        this.filename = filename;
        this.mark = mark;
        this.fileUid = filename.getBytes(StandardCharsets.UTF_8);
    }

    protected RecordUid(byte[] fileUid, long mark) {
        this.filename = new String(fileUid, StandardCharsets.UTF_8);
        this.mark = mark;
        this.fileUid = fileUid;
    }

    public byte[] toBytes() {
        ByteBuffer markBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(mark);
        byte[] uid = Arrays.copyOf(fileUid, fileUid.length + 8);
        System.arraycopy(markBytes.array(), 0, uid, fileUid.length, 8);
        return uid;
    }

    public static RecordUid fromBytes(byte[] uid) throws AppException {
        if (uid.length < 8 + 1) {
            throw new AppException("invalid record uid");
        }
        long mark = ByteBuffer.wrap(uid).order(ByteOrder.BIG_ENDIAN).getLong(uid.length - 8);
        byte[] fileUid = new byte[uid.length - 8];
        System.arraycopy(uid, 0, fileUid, 0, uid.length - 8);
        return new RecordUid(fileUid, mark);
    }

    @Override
    public String toString() {
        return filename + ":" + mark;
    }

    /**
     * Convert a list of uids to a map positions per file
     * 
     * @param uids the list of uids
     * @return the map
     * @throws AppException
     */
    public static HashMap<String, List<Long>> positionsPerFile(Set<byte[]> uids) throws AppException {
        HashMap<String, List<Long>> positionsPerFile = new HashMap<>();
        for (byte[] uid : uids) {
            RecordUid recordUid = RecordUid.fromBytes(uid);
            positionsPerFile.compute(recordUid.filename, (fn, pos) -> {
                List<Long> list = (pos == null) ? new ArrayList<>() : pos;
                list.add(recordUid.mark);
                return list;
            });
        }
        return positionsPerFile;
    }

}
