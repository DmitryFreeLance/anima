package ru.animabot;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Base64;

/**
 * Webhook Prodamus:
 *  - Проверяем подпись провайдера по RAW body (HEX/base64/"sha256=...").
 *  - Достаём order_id → находим userId/plan/days (таблица orders).
 *  - Валидируем нашу sw_sig = HMAC(uid:plan:order_id) по BOT_LINK_SECRET.
 *  - Идемпотентность по payment_id (или order_id).
 *  - Выдаём подписку через bot.onProdamusPaid(uid, days) и помечаем заказ оплаченным.
 */
public class ProdamusWebhookServer {

    private static final Logger LOG = LoggerFactory.getLogger(ProdamusWebhookServer.class);

    private final HttpServer server;
    private final SoulWayBot bot;
    private final String secret;

    public ProdamusWebhookServer(SoulWayBot bot, int port, String secret) throws IOException {
        this.bot = bot;
        this.secret = (secret == null) ? "" : secret.trim();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/webhook/prodamus", new Handler());
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
    }

    public void start() {
        server.start();
        LOG.info("Prodamus webhook is listening at /webhook/prodamus");
    }

    private class Handler implements HttpHandler {
        @Override public void handle(HttpExchange ex) throws IOException {
            long t0 = System.currentTimeMillis();
            try {
                if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                    respond(ex, 405, "method not allowed");
                    return;
                }

                if (secret.isBlank()) {
                    LOG.error("[prodamus] PRODAMUS_SECRET is empty");
                    respond(ex, 500, "server misconfigured");
                    return;
                }

                String botLinkSecret = System.getenv().getOrDefault("BOT_LINK_SECRET", "");
                if (botLinkSecret.isBlank()) {
                    LOG.error("[prodamus] BOT_LINK_SECRET is empty");
                    respond(ex, 500, "server misconfigured");
                    return;
                }

                byte[] raw = readAll(ex.getRequestBody());
                String ctype = Optional.ofNullable(ex.getRequestHeaders().getFirst("Content-Type")).orElse("");
                String sigHeader = firstNonEmpty(
                        ex.getRequestHeaders().getFirst("Sign"),
                        ex.getRequestHeaders().getFirst("Signature"),
                        ex.getRequestHeaders().getFirst("X-Signature"),
                        ex.getRequestHeaders().getFirst("X-Webhook-Signature"),
                        null);

                LOG.info("[prodamus] incoming: ct='{}', sigPresent={}, rawLen={}",
                        shortCt(ctype), sigHeader != null, raw.length);

                String body = new String(raw, StandardCharsets.UTF_8);
                Map<String, Object> form = parseForm(body);

                // подпись провайдера
                if (!verifySignatureFlexible(raw, sigHeader, secret)) {
                    LOG.warn("[prodamus] provider signature mismatch; flatSan={}", toFlatSanitized(form));
                    respond(ex, 400, "bad signature");
                    return;
                }

                String orderId = getString(form, "order_id");
                SQLiteManager.OrderInfo order = (orderId == null || orderId.isBlank())
                        ? null
                        : bot.getDb().getOrder(orderId);

                long uid = (order != null) ? order.getUserId() : parseLong(getString(form, "customer_extra"), -1);
                int  plan = (order != null) ? order.getPlan()    : (int) parseLong(getString(form, "plan"), -1);

                String swSig = getString(form, "sw_sig");
                if (uid <= 0 || plan <= 0 || swSig == null || swSig.isBlank() || orderId == null || orderId.isBlank()) {
                    LOG.warn("[prodamus] invalid payload; flatSan={}", toFlatSanitized(form));
                    respond(ex, 400, "bad request");
                    return;
                }

                String localSig = SoulWayBot.hmacSha256Hex(uid + ":" + plan + ":" + orderId, botLinkSecret);
                if (!constantTimeEqualsIgnoreCase(localSig, stripSha256Prefix(swSig))) {
                    LOG.warn("[prodamus] sw_sig check failed");
                    respond(ex, 400, "bad signature (sw_sig)");
                    return;
                }

                String eventId = firstNonEmpty(getString(form, "payment_id"), orderId);
                boolean firstTime = bot.getDb().markWebhookProcessed("prodamus", eventId);
                if (!firstTime) {
                    LOG.info("[prodamus] duplicate webhook: {}", eventId);
                    respond(ex, 200, "ok (duplicate)");
                    return;
                }

                int days = (order != null) ? order.getDays()
                        : (plan == 1 ? safeParseInt(bot.getDb().getSetting("tariff1_days", "30"), 30)
                        : (plan == 2 ? safeParseInt(bot.getDb().getSetting("tariff2_days", "90"), 90)
                        :               safeParseInt(bot.getDb().getSetting("tariff3_days", "365"), 365)));

                if (orderId != null) bot.getDb().markOrderPaid(orderId);

                bot.onProdamusPaid(uid, days);

                respond(ex, 200, "ok");
            } catch (Exception e) {
                LOG.error("webhook error", e);
                respond(ex, 500, "internal error");
            } finally {
                LOG.info("[prodamus] handled in {} ms", (System.currentTimeMillis() - t0));
            }
        }
    }

    // ===== helpers =====

    /** HEX/base64/"sha256=..." по raw body. */
    private static boolean verifySignatureFlexible(byte[] rawBody, String signature, String secret) {
        if (signature == null || signature.isBlank()) return false;
        String sig = stripSha256Prefix(signature.trim());
        try {
            byte[] calc = hmacSha256(rawBody, secret.getBytes(StandardCharsets.UTF_8));
            String hex = toHex(calc);
            String b64 = Base64.getEncoder().encodeToString(calc);
            return constantTimeEqualsIgnoreCase(hex, sig) || constantTimeEquals(b64, sig);
        } catch (Exception e) {
            return false;
        }
    }

    private static String stripSha256Prefix(String s) {
        String v = s == null ? "" : s.trim();
        return v.toLowerCase(Locale.ROOT).startsWith("sha256=") ? v.substring(7).trim() : v;
    }

    private static byte[] hmacSha256(byte[] data, byte[] key) throws Exception {
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data != null ? data : new byte[0]);
    }

    private static String toHex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }

    private static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) return false;
        if (a.length() != b.length()) return false;
        int r = 0;
        for (int i = 0; i < a.length(); i++) r |= a.charAt(i) ^ b.charAt(i);
        return r == 0;
    }

    private static boolean constantTimeEqualsIgnoreCase(String aHex, String bHex) {
        if (aHex == null || bHex == null) return false;
        if (aHex.length() != bHex.length()) return false;
        int r = 0;
        for (int i = 0; i < aHex.length(); i++) {
            char ca = Character.toLowerCase(aHex.charAt(i));
            char cb = Character.toLowerCase(bHex.charAt(i));
            r |= ca ^ cb;
        }
        return r == 0;
    }

    private static String firstNonEmpty(String... v) {
        if (v == null) return null;
        for (String s : v) if (s != null && !s.isBlank()) return s;
        return null;
    }

    private static String getString(Map<String, Object> map, String key) {
        Object v = map.get(key);
        if (v == null) return null;
        if (v instanceof String) return (String) v;
        return String.valueOf(v);
    }

    private static String shortCt(String ct) {
        int i = ct.indexOf(';');
        return i >= 0 ? ct.substring(0, i).trim() : ct.trim();
    }

    private static Map<String, Object> parseForm(String body) throws UnsupportedEncodingException {
        Map<String, Object> map = new HashMap<>();
        if (body == null || body.isBlank()) return map;
        for (String pair : body.split("&")) {
            int i = pair.indexOf('=');
            if (i < 0) continue;
            String k = URLDecoder.decode(pair.substring(0, i), "UTF-8");
            String v = URLDecoder.decode(pair.substring(i + 1), "UTF-8");
            map.put(k, v);
        }
        return map;
    }

    private static long parseLong(String s, long def) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return def; }
    }

    private static int safeParseInt(String s, int def) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e){ return def; }
    }

    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int r;
        while ((r = in.read(buf)) >= 0) bos.write(buf, 0, r);
        return bos.toByteArray();
    }

    private static void respond(HttpExchange ex, int code, String txt) throws IOException {
        byte[] out = txt.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(code, out.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(out); }
        ex.close();
    }

    private static String toFlatSanitized(Map<String, Object> map) {
        if (map == null) return "{}";
        Map<String, Object> copy = new HashMap<>(map);
        for (String k : Arrays.asList("phone","customer_phone","email","customer_email","card","pan")) {
            if (copy.containsKey(k)) copy.put(k, "***");
        }
        return copy.toString();
    }
}
