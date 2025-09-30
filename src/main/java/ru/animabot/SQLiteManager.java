package ru.animabot;

import java.sql.*;
import java.util.*;

/**
 * SQLite менеджер:
 *  - keywords: ключевые слова
 *  - settings: произвольные настройки (ключ/значение)
 *  - subscriptions: подписки пользователей (userId -> expiresAtMillis)
 */
public class SQLiteManager {

    private final String dbUrl;

    public SQLiteManager(String dbFile) {
        this.dbUrl = "jdbc:sqlite:" + (dbFile == null || dbFile.isBlank() ? "soulway.db" : dbFile);
        ensureSchema();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(dbUrl);
    }

    /** Проверка/создание/миграция схемы. */
    private void ensureSchema() {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            // keywords
            if (!tableExists(conn, "keywords")) {
                try (Statement st = conn.createStatement()) {
                    st.executeUpdate("CREATE TABLE IF NOT EXISTS keywords (" +
                            "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                            "keyword TEXT UNIQUE NOT NULL," +
                            "introText TEXT," +
                            "rewardText TEXT," +
                            "materials TEXT" +
                            ");");
                }
            } else {
                Set<String> cols = getTableColumns(conn, "keywords");
                if (!cols.containsAll(Set.of("keyword","introText","rewardText","materials"))) {
                    try (Statement st = conn.createStatement()) {
                        st.executeUpdate("DROP TABLE IF EXISTS keywords");
                        st.executeUpdate("CREATE TABLE keywords (" +
                                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                                "keyword TEXT UNIQUE NOT NULL," +
                                "introText TEXT," +
                                "rewardText TEXT," +
                                "materials TEXT" +
                                ");");
                    }
                }
            }

            // settings
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS settings (" +
                        "key TEXT PRIMARY KEY," +
                        "value TEXT" +
                        ");");
            }

            // subscriptions
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS subscriptions (" +
                        "userId INTEGER PRIMARY KEY," +
                        "expiresAtMillis INTEGER NOT NULL" +
                        ");");
            }

            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean tableExists(Connection conn, String table) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name=?")) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    private Set<String> getTableColumns(Connection conn, String table) throws SQLException {
        Set<String> cols = new HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement("PRAGMA table_info(" + table + ")")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("name");
                    if (name != null) cols.add(name);
                }
            }
        }
        return cols;
    }

    // ----------------- keywords API -----------------

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

    // ----------------- settings API -----------------

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

    // ----------------- subscription API -----------------

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

    // ----------------- utils -----------------

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
}
