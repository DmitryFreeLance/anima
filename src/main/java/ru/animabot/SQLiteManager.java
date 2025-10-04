package ru.animabot;

import java.sql.*;
import java.util.*;

/**
 * SQLite:
 *  - keywords (keyword, introText, rewardText, materials)
 *  - settings (key,value)
 *  - subscriptions (userId, expiresAtMillis)
 *  - orders (orderId, userId, plan, days, createdAtMillis, paidAtMillis)
 *  - processed_webhooks (provider, event_id) — идемпотентность
 *  - drip_campaigns (userId, nextAtMillis, step)
 * Включены WAL/busy_timeout.
 */
public class SQLiteManager {

    private final String dbUrl;

    public SQLiteManager(String dbFile) {
        this.dbUrl = "jdbc:sqlite:" + (dbFile == null || dbFile.isBlank() ? "soulway.db" : dbFile);
        ensureSchema();
    }

    private Connection connect() throws SQLException {
        Connection conn = DriverManager.getConnection(dbUrl);
        try (Statement st = conn.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL;");
            st.execute("PRAGMA busy_timeout=5000;");
            st.execute("PRAGMA synchronous=NORMAL;");
        }
        return conn;
    }

    private void ensureSchema() {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);

            try (Statement st = conn.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS keywords (" +
                        "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                        "keyword TEXT UNIQUE NOT NULL," +
                        "introText TEXT," +
                        "rewardText TEXT," +
                        "materials TEXT" +
                        ");");
            }

            try (Statement st = conn.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS settings (" +
                        "key TEXT PRIMARY KEY," +
                        "value TEXT" +
                        ");");
            }

            try (Statement st = conn.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS subscriptions (" +
                        "userId INTEGER PRIMARY KEY," +
                        "expiresAtMillis INTEGER NOT NULL" +
                        ");");
            }

            try (Statement st = conn.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS orders (" +
                        "orderId TEXT PRIMARY KEY," +
                        "userId INTEGER NOT NULL," +
                        "plan INTEGER NOT NULL," +
                        "days INTEGER NOT NULL," +
                        "createdAtMillis INTEGER NOT NULL," +
                        "paidAtMillis INTEGER" +
                        ");");
            }

            try (Statement st = conn.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS processed_webhooks (" +
                        "provider TEXT NOT NULL," +
                        "event_id TEXT NOT NULL," +
                        "processed_at INTEGER NOT NULL," +
                        "PRIMARY KEY(provider, event_id)" +
                        ");");
            }

            // Новая таблица для drip-кампаний
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS drip_campaigns (" +
                        "userId INTEGER PRIMARY KEY," +
                        "nextAtMillis INTEGER NOT NULL," +
                        "step INTEGER NOT NULL" +
                        ");");
            }

            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // ===== keywords =====

    public void upsertKeyword(Keyword kw) {
        if (kw == null || kw.getKeyword() == null || kw.getKeyword().isBlank()) return;
        String sql = "INSERT INTO keywords(keyword, introText, rewardText, materials) VALUES(?,?,?,?) " +
                "ON CONFLICT(keyword) DO UPDATE SET introText=excluded.introText, " +
                "rewardText=excluded.rewardText, materials=excluded.materials";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, normalizeKey(kw.getKeyword()));
            ps.setString(2, nullIfBlank(kw.getIntroText()));
            ps.setString(3, nullIfBlank(kw.getRewardText()));
            ps.setString(4, materialsToCsv(kw.getMaterials()));
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Keyword findKeywordByKey(String key) {
        if (key == null || key.isBlank()) return null;
        String sql = "SELECT keyword, introText, rewardText, materials FROM keywords WHERE keyword = ?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, normalizeKey(key));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Keyword kw = new Keyword();
                    kw.setKeyword(rs.getString("keyword"));
                    kw.setIntroText(rs.getString("introText"));
                    kw.setRewardText(rs.getString("rewardText"));
                    kw.setMaterials(csvToList(rs.getString("materials")));
                    return kw;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Keyword> listKeywords() {
        List<Keyword> list = new ArrayList<>();
        String sql = "SELECT keyword, introText, rewardText, materials FROM keywords ORDER BY id DESC";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Keyword kw = new Keyword();
                kw.setKeyword(rs.getString("keyword"));
                kw.setIntroText(rs.getString("introText"));
                kw.setRewardText(rs.getString("rewardText"));
                kw.setMaterials(csvToList(rs.getString("materials")));
                list.add(kw);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    // ===== settings =====

    public void setSetting(String key, String value) {
        String sql = "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            ps.setString(2, value);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String getSetting(String key, String def) {
        String sql = "SELECT value FROM settings WHERE key=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("value");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return def;
    }

    // ===== subscriptions =====

    public void grantSubscription(long userId, int days) {
        long now = System.currentTimeMillis();
        long add = days * 24L * 60L * 60L * 1000L;
        long newExp = now + add;
        Long cur = getSubscriptionExpiry(userId);
        if (cur != null && cur > now) newExp = cur + add;

        String sql = "INSERT INTO subscriptions(userId, expiresAtMillis) VALUES(?,?) " +
                "ON CONFLICT(userId) DO UPDATE SET expiresAtMillis=excluded.expiresAtMillis";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, userId);
            ps.setLong(2, newExp);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void grantSubscriptionMinutes(long userId, int minutes) {
        long now = System.currentTimeMillis();
        long add = minutes * 60L * 1000L;
        long newExp = now + add;
        Long cur = getSubscriptionExpiry(userId);
        if (cur != null && cur > now) newExp = cur + add;

        String sql = "INSERT INTO subscriptions(userId, expiresAtMillis) VALUES(?,?) " +
                "ON CONFLICT(userId) DO UPDATE SET expiresAtMillis=excluded.expiresAtMillis";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, userId);
            ps.setLong(2, newExp);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void revokeSubscription(long userId) {
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement("DELETE FROM subscriptions WHERE userId=?")) {
            ps.setLong(1, userId);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Long getSubscriptionExpiry(long userId) {
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement("SELECT expiresAtMillis FROM subscriptions WHERE userId=?")) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong("expiresAtMillis");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Long> listExpiredSince(long timestampMillis) {
        List<Long> list = new ArrayList<>();
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement("SELECT userId FROM subscriptions WHERE expiresAtMillis<?")) {
            ps.setLong(1, timestampMillis);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) list.add(rs.getLong("userId"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    // ===== orders / webhooks =====

    public String createPendingOrder(long userId, int plan, int days) {
        String orderId = UUID.randomUUID().toString().replace("-", "");
        String sql = "INSERT INTO orders(orderId, userId, plan, days, createdAtMillis, paidAtMillis) VALUES(?,?,?,?,?,NULL)";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, orderId);
            ps.setLong(2, userId);
            ps.setInt(3, plan);
            ps.setInt(4, days);
            ps.setLong(5, System.currentTimeMillis());
            ps.executeUpdate();
            return orderId;
        } catch (SQLException e) {
            e.printStackTrace();
            return orderId; // даже если не вставилось, вернём id для диагностики
        }
    }

    public OrderInfo getOrder(String orderId) {
        if (orderId == null || orderId.isBlank()) return null;
        String sql = "SELECT userId, plan, days, createdAtMillis, paidAtMillis FROM orders WHERE orderId=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, orderId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long paid = rs.getLong("paidAtMillis");
                    Long paidNullable = rs.wasNull() ? null : paid;
                    return new OrderInfo(
                            orderId,
                            rs.getLong("userId"),
                            rs.getInt("plan"),
                            rs.getInt("days"),
                            rs.getLong("createdAtMillis"),
                            paidNullable
                    );
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean markOrderPaid(String orderId) {
        String sql = "UPDATE orders SET paidAtMillis = COALESCE(paidAtMillis, ?) WHERE orderId = ?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, orderId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    /** true — если событие видим впервые. */
    public boolean markWebhookProcessed(String provider, String eventId) {
        if (provider == null || eventId == null) return false;
        String sql = "INSERT OR IGNORE INTO processed_webhooks(provider, event_id, processed_at) VALUES(?,?,?)";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, provider);
            ps.setString(2, eventId);
            ps.setLong(3, System.currentTimeMillis());
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    // ===== drip-campaigns =====

    public static class Drip {
        public final long userId;
        public final long nextAtMillis;
        public final int step;
        public Drip(long userId, long nextAtMillis, int step) {
            this.userId = userId; this.nextAtMillis = nextAtMillis; this.step = step;
        }
    }

    public void startOrResetDrip(long userId, long nextAtMillis, int step) {
        String sql = "INSERT INTO drip_campaigns(userId, nextAtMillis, step) VALUES(?,?,?) " +
                "ON CONFLICT(userId) DO UPDATE SET nextAtMillis=excluded.nextAtMillis, step=excluded.step";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, userId);
            ps.setLong(2, nextAtMillis);
            ps.setInt(3, step);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void updateDrip(long userId, long nextAtMillis, int step) {
        String sql = "UPDATE drip_campaigns SET nextAtMillis=?, step=? WHERE userId=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, nextAtMillis);
            ps.setInt(2, step);
            ps.setLong(3, userId);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteDrip(long userId) {
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement("DELETE FROM drip_campaigns WHERE userId=?")) {
            ps.setLong(1, userId);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<Drip> listDueDrips(long nowMillis, int limit) {
        List<Drip> out = new ArrayList<>();
        String sql = "SELECT userId, nextAtMillis, step FROM drip_campaigns WHERE nextAtMillis<=? ORDER BY nextAtMillis ASC LIMIT ?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, nowMillis);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Drip(rs.getLong("userId"), rs.getLong("nextAtMillis"), rs.getInt("step")));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return out;
    }

    // ===== utils =====

    private static String normalizeKey(String s) { return s == null ? null : s.trim().toUpperCase(); }
    private static String nullIfBlank(String s) { return (s == null || s.isBlank()) ? null : s; }

    private static String materialsToCsv(List<String> mats) {
        if (mats == null || mats.isEmpty()) return null;
        StringBuilder sb = new StringBuilder();
        for (String m : mats) {
            if (m == null) continue;
            String v = m.trim();
            if (v.isEmpty()) continue;
            if (sb.length() > 0) sb.append(",");
            sb.append(v);
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static List<String> csvToList(String csv) {
        List<String> out = new ArrayList<>();
        if (csv == null || csv.isBlank()) return out;
        for (String s : csv.split(",")) {
            String v = s.trim();
            if (!v.isEmpty()) out.add(v);
        }
        return out;
    }

    // DTO заказа
    public static class OrderInfo {
        private final String orderId;
        private final long userId;
        private final int plan;
        private final int days;
        private final long createdAtMillis;
        private final Long paidAtMillis;

        public OrderInfo(String orderId, long userId, int plan, int days, long createdAtMillis, Long paidAtMillis) {
            this.orderId = orderId;
            this.userId = userId;
            this.plan = plan;
            this.days = days;
            this.createdAtMillis = createdAtMillis;
            this.paidAtMillis = paidAtMillis;
        }

        public String getOrderId() { return orderId; }
        public long getUserId() { return userId; }
        public int getPlan() { return plan; }
        public int getDays() { return days; }
        public long getCreatedAtMillis() { return createdAtMillis; }
        public Long getPaidAtMillis() { return paidAtMillis; }
    }
}