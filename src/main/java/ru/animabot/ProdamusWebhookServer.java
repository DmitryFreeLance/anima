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

public class ProdamusWebhookServer {

    private static final Logger LOG = LoggerFactory.getLogger(ProdamusWebhookServer.class);

    private final HttpServer server;
    private final SoulWayBot bot;
    private final String providerSecret; // секрет подписи провайдера (если есть)
    private final String linkSecret;     // наш секрет для токена swb:<uid>:<days>:<hmac>

    public ProdamusWebhookServer(SoulWayBot bot, int port, String providerSecret) throws IOException {
        this.bot = bot;
        this.providerSecret = (providerSecret == null) ? "" : providerSecret.trim();
        this.linkSecret = System.getenv().getOrDefault("BOT_LINK_SECRET", "");
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/webhook/prodamus", new Handler());
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
    }

    public void start() {
        server.start();
        LOG.info("Prodamus Webhook server started at /webhook/prodamus");
    }

    private class Handler implements HttpHandler {
        @Override public void handle(HttpExchange ex) throws IOException {
            long t0 = System.currentTimeMillis();
            try {
                if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                    respond(ex, 405, "method not allowed");
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

                boolean strict = !"0".equals(System.getenv().getOrDefault("PRODAMUS_STRICT", "1"));
                if (!providerSecret.isBlank()) {
                    if (!verifySignatureFlexible(raw, sigHeader, providerSecret)) {
                        Map<String,Object> flatSan = parseForm(new String(raw, StandardCharsets.UTF_8));
                        LOG.warn("[prodamus] provider signature mismatch; strict={}, flatSan={}", strict, toFlat(flatSan));
                        if (strict) {
                            respond(ex, 200, "ok (ignored)");
                            return;
                        }
                        // strict=0 → продолжаем обработку
                    }
                } else {
                    LOG.warn("[prodamus] PRODAMUS_SECRET is empty — provider signature check is DISABLED!");
                }

                String body = new String(raw, StandardCharsets.UTF_8);
                Map<String, Object> form = parseForm(body);

                // Поля (двойная декодировка значений — часто приходят проценто-кодированные)
                String status      = or(getString(form, "payment_status"), getString(form, "status"));
                String orderId     = doubleDecode(getString(form, "order_id"));
                String orderNum    = doubleDecode(getString(form, "order_num"));     // ← здесь обычно наш токен
                String custExtra   = doubleDecode(getString(form, "customer_extra"));
                String sumStr      = doubleDecode(getString(form, "sum"));
                String prodPrice0  = doubleDecode(getString(form, "products[0][price]"));
                String prodName0   = doubleDecode(getString(form, "products[0][name]"));

                LOG.info("[prodamus] parsed: order_id='{}', order_num='{}', customer_extra='{}', sum='{}', price0='{}', name0='{}', status='{}'",
                        safe(orderId), safe(orderNum), safe(custExtra), safe(sumStr), safe(prodPrice0), safe(prodName0), safe(status));

                if (!"success".equalsIgnoreCase(safe(status))) {
                    respond(ex, 200, "ok (ignored)");
                    return;
                }

                // 1) Пытаемся разобрать наш токен swb:<uid>:<days>:<hmac> (сначала order_num, потом order_id)
                long[] parsed = tryParseOurToken(orderNum);
                if (parsed == null) parsed = tryParseOurToken(orderId);

                if (parsed != null) {
                    long uid = parsed[0];
                    int  days = (int) parsed[1];
                    try {
                        bot.onProdamusPaid(uid, days);
                        respond(ex, 200, "ok");
                    } catch (Exception e) {
                        LOG.error("onProdamusPaid failed (token path)", e);
                        respond(ex, 500, "internal error");
                    }
                    LOG.info("[prodamus] handled in {} ms (token path uid={}, days={})",
                            (System.currentTimeMillis() - t0), parsed[0], parsed[1]);
                    return;
                }

                // 2) Fallback — если токена нет: берём uid из customer_extra и пытаемся вывести days по цене/именованию
                long uid = parseLong(custExtra, -1);
                if (uid > 0) {
                    Integer days = null;
                    days = (days != null) ? days : mapPriceToDays(sumStr);
                    days = (days != null) ? days : mapPriceToDays(prodPrice0);
                    days = (days != null) ? days : mapNameToDays(prodName0);

                    if (days != null && days > 0) {
                        try {
                            bot.onProdamusPaid(uid, days);
                            respond(ex, 200, "ok");
                        } catch (Exception e) {
                            LOG.error("onProdamusPaid failed (fallback path)", e);
                            respond(ex, 500, "internal error");
                        }
                        LOG.info("[prodamus] handled in {} ms (fallback uid={}, days={})",
                                (System.currentTimeMillis() - t0), uid, days);
                        return;
                    } else {
                        LOG.warn("[prodamus] fallback could not determine days (uid present). form={}", toFlat(form));
                        respond(ex, 200, "ok (ignored)");
                        return;
                    }
                }

                LOG.warn("[prodamus] insufficient data to grant sub; flat={}", toFlat(form));
                respond(ex, 200, "ok (ignored)");

            } catch (Exception e) {
                LOG.error("webhook error", e);
                respond(ex, 500, "internal error");
            } finally {
                LOG.info("[prodamus] handled in {} ms", (System.currentTimeMillis() - t0));
            }
        }
    }

    // ==== Наш токен может лежать в order_num или order_id ====
    private long[] tryParseOurToken(String v) {
        if (v == null || v.isBlank()) return null;
        return SoulWayBot.parseOrderIdToken(v.trim(), linkSecret);
    }

    // ==== Двойная URL-декодировка ====
    private static String doubleDecode(String s) {
        if (s == null) return null;
        String once = urlDecodeSafe(s);
        String twice = urlDecodeSafe(once);
        return twice;
    }
    private static String urlDecodeSafe(String s) {
        try { return URLDecoder.decode(s, StandardCharsets.UTF_8); }
        catch (Exception e) { return s; }
    }

    // ==== Определение срока по цене/названию (fallback) ====
    private Integer mapPriceToDays(String priceStr) {
        int price = parseInt(roundPrice(priceStr), -1);
        if (price <= 0) return null;
        if (price == 1299)  return 30;
        if (price == 3599)  return 90;
        if (price == 12900) return 365;
        return null;
    }
    private String roundPrice(String s) {
        if (s == null) return null;
        String digits = s.replaceAll("[^0-9]", "");
        return digits.isEmpty() ? null : digits;
    }
    private Integer mapNameToDays(String name) {
        if (name == null) return null;
        String n = name.toLowerCase(Locale.ROOT);
        if (n.contains("1 месяц") || n.contains("1 мес"))  return 30;
        if (n.contains("3 месяца") || n.contains("3 мес")) return 90;
        if (n.contains("12 месяцев") || n.contains("12 мес")) return 365;
        int months = extractMonths(n);
        if (months == 1)  return 30;
        if (months == 3)  return 90;
        if (months == 12) return 365;
        return null;
    }
    private int extractMonths(String n) {
        try {
            int idx = n.indexOf("мес");
            if (idx < 0) idx = n.indexOf("месяц");
            if (idx < 0) idx = n.indexOf("месяцев");
            if (idx > 0) {
                int i = idx - 1;
                while (i >= 0 && Character.isWhitespace(n.charAt(i))) i--;
                int end = i + 1;
                while (i >= 0 && Character.isDigit(n.charAt(i))) i--;
                int start = i + 1;
                if (start < end) return Integer.parseInt(n.substring(start, end));
            }
        } catch (Exception ignore) {}
        return -1;
    }

    // ==== helpers ====
    private static boolean verifySignatureFlexible(byte[] rawBody, String signature, String secret) {
        if (signature == null || signature.isBlank()) return false;
        String sig = signature.trim();
        try {
            byte[] calc = hmacSha256(rawBody, secret.getBytes(StandardCharsets.UTF_8));
            String hex = toHex(calc);
            String b64 = Base64.getEncoder().encodeToString(calc);
            String s = sig.toLowerCase(Locale.ROOT).startsWith("sha256=") ? sig.substring(7).trim() : sig;
            return constantTimeEquals(hex, s) || constantTimeEquals(b64, s);
        } catch (Exception e) { return false; }
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
        for (int i = 0; i < a.length(); i++) r |= a.charAt(i) ^ a.charAt(i);
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
        Map<String, Object> map = new LinkedHashMap<>();
        if (body == null || body.isBlank()) return map;
        for (String pair : body.split("&")) {
            int i = pair.indexOf('=');
            if (i < 0) continue;
            String k = URLDecoder.decode(pair.substring(0, i), StandardCharsets.UTF_8);
            String v = URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8);
            map.put(k, v);
        }
        return map;
    }

    private static long parseLong(String s, long def) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return def; }
    }
    private static int parseInt(String s, int def) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return def; }
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

    private static String toFlat(Map<String, Object> map) {
        if (map == null) return "{}";
        return map.toString();
    }

    private static String or(String a, String b) { return a != null ? a : b; }
    private static String safe(String s) { return s == null ? "" : s; }
}