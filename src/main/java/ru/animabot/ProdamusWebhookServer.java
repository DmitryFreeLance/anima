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

/**
 * Простой HTTP-сервер для приёма webhook от Prodamus:
 *  - endpoint: POST /webhook/prodamus
 *  - формат: application/x-www-form-urlencoded
 *  - подпись: HMAC-SHA256 (секрет из PRODAMUS_SECRET) по сырому телу
 *
 * Мы кладём в ссылку параметр customer_extra=<Telegram userId> и days=<число дней>.
 * Prodamus возвращает customer_extra как есть, поэтому по нему находим пользователя.
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
                        ex.getRequestHeaders().getFirst("Signature"),
                        ex.getRequestHeaders().getFirst("X-Signature"),
                        ex.getRequestHeaders().getFirst("X-Webhook-Signature"),
                        null);

                LOG.info("[prodamus] incoming: ct='{}', sigHeader='{}', rawLen={} bytes",
                        shortCt(ctype), sigHeader == null ? "absent" : "present", raw.length);

                // Проверка подписи (если секрет задан)
                if (!secret.isBlank()) {
                    if (!verifySignatureFlexible(raw, sigHeader, secret)) {
                        LOG.warn("[prodamus] bad signature -> reject");
                        respond(ex, 400, "bad signature");
                        return;
                    }
                } else {
                    LOG.warn("[prodamus] PRODAMUS_SECRET is empty — signature check is DISABLED!");
                }

                String body = new String(raw, StandardCharsets.UTF_8);
                Map<String, Object> form = parseForm(body);

                // читаем uid из customer_extra (как мы передали в платёжной ссылке)
                long uid = parseLong(getString(form, "customer_extra"), -1);
                int days = (int) parseLong(getString(form, "days"), 30);

                LOG.info("[prodamus] parsed: uid={}, days={}", uid, days);

                if (uid > 0) {
                    try {
                        bot.onProdamusPaid(uid, days);
                        respond(ex, 200, "ok");
                    } catch (Exception e) {
                        LOG.error("onProdamusPaid failed", e);
                        respond(ex, 500, "internal error");
                    }
                } else {
                    LOG.warn("[prodamus] webhook without uid; flat={}", toFlat(form));
                    respond(ex, 200, "ok (ignored)");
                }
            } catch (Exception e) {
                LOG.error("webhook error", e);
                respond(ex, 500, "internal error");
            } finally {
                LOG.info("[prodamus] handled in {} ms", (System.currentTimeMillis() - t0));
            }
        }
    }

    // ===== helpers =====

    private static boolean verifySignatureFlexible(byte[] rawBody, String signature, String secret) {
        if (signature == null || signature.isBlank()) return false;
        String sig = signature.trim();

        // Поддержим форматы:
        //  - HEX
        //  - base64
        //  - "sha256=..." (GitHub-style)
        try {
            byte[] calc = hmacSha256(rawBody, secret.getBytes(StandardCharsets.UTF_8));
            String hex = toHex(calc);
            String b64 = Base64.getEncoder().encodeToString(calc);

            String sigStr = sig;
            if (sigStr.toLowerCase(Locale.ROOT).startsWith("sha256=")) {
                sigStr = sigStr.substring("sha256=".length()).trim();
            }

            // сравним с hex и base64
            return constantTimeEquals(hex, sigStr) || constantTimeEquals(b64, sigStr);
        } catch (Exception e) {
            return false;
        }
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
            String k = URLDecoder.decode(pair.substring(0, i), StandardCharsets.UTF_8);
            String v = URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8);

            // Поддержка скобочных имён (products[0][name]) — кладём плоско
            map.put(k, v);
        }
        return map;
    }

    private static long parseLong(String s, long def) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return def; }
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
}