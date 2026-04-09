public record CachedValue(String value, long expirationTimeMs) {
    
    public boolean isExpired() {
        if (expirationTimeMs == -1) {
            return false;
        }
        return System.currentTimeMillis() > expirationTimeMs;
    }
}
