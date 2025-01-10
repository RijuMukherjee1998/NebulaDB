package Utility;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class ChecksumGen {
    public byte[] generateChecksum(byte[] data) {
        try {
            // Get an instance of the SHA-256 algorithm
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }

    public boolean matchChecksum(byte[] data, byte[] checksum) {
        if(checksum == null || checksum.length != data.length) {
            return false;
        }
        for (int i = 0; i < data.length; i++) {
            if (data[i] != checksum[i]) {
                return false;
            }
        }
        return true;
    }
}
