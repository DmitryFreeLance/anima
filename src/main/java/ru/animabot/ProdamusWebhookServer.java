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
 *  - формат: application/x-www-form-urlencoded (обычный кейс)
 *  - подпись: HMAC-SHA256 (секрет из PRODAMUS_SECRET)
 *
 * Примечания:
 *  - Prodamus присылает в webhook параметры, включая те, что вы передали в ссылке оплаты.
 *    Мы добавляем к ссылке ?order_id=<uid>&days=<N>, забираем их из webhook и активируем подписку.
 *  - Поле с подписью/заголовок см. в кабинете Prodamus. Здесь проверяем:
 *      1) header "Signature" (или "X-Signature"),
 *      2) либо form-параметр "signature".
 *    Алгоритм — HMAC-SHA256 по сырому телу (raw body).
 *    Если ваш кабинет требует иной способ (по отсортированным полям и т.п.) —
 *    адаптируйте метод verifySignature(...) по документации.
 */
public class ProdamusWebhookServer {

    private static final Logger LOG = LoggerFactory.getLogger(ProdamusWebhookServer.class);

    private final HttpServer server;
    private final SoulWayBot bot;
    private final String secret;

    public ProdamusWebhookServer(SoulWayBot bot, int port, String secret) throws IOException {
        this.bot = bot;
        this.secret = secret == null ? "" : secret;
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
            try {
                if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                    respond(ex, 405, "method not allowed"); return;
                }
                byte[] raw = readAll(ex.getRequestBody());
                String body = new String(raw, StandardCharsets.UTF_8);

                // подпись: пытаемся взять из заголовков либо из формы
                String gotSig = firstNonEmpty(
                        ex.getRequestHeaders().getFirst("Signature"),
                        ex.getRequestHeaders().getFirst("X-Signature"),
                        ex.getRequestHeaders().getFirst("X-Webhook-Signature"),
                        null);

                Map<String, String> form = parseForm(body);

                if (gotSig == null) gotSig = form.get("signature");

                if (!secret.isBlank()) {
                    if (!verifySignature(raw, gotSig, secret)) {
                        LOG.warn("bad signature, reject");
                        respond(ex, 400, "bad signature");
                        return;
                    }
                } else {
                    LOG.warn("PRODAMUS_SECRET is empty — signature check is disabled!");
                }

                // типичное успешное событие: ищем order_id и days
                long uid = parseLong(form.get("order_id"), -1);
                int days = (int) parseLong(form.getOrDefault("days", "30"), 30);

                // Если order_id не пришёл — пробуем альтернативные поля (настрой зависит от кабинета)
                if (uid <= 0) {
                    uid = parseLong(form.get("customer_extra"), -1); // пример альтернативы
                }

                if (uid > 0) {
                    bot.onProdamusPaid(uid, days);
                    respond(ex, 200, "ok");
                } else {
                    LOG.warn("Webhook without order_id: {}", body);
                    respond(ex, 200, "ok (ignored)");
                }
            } catch (Exception e) {
                LOG.error("webhook error", e);
                respond(ex, 500, "internal error");
            }
        }
    }

    // ===== helpers =====

    private static boolean verifySignature(byte[] rawBody, String signature, String secret) {
        if (signature == null) return false;
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] calc = mac.doFinal(rawBody != null ? rawBody : new byte[0]);
            String hex = toHex(calc);
            return constantTimeEquals(hex, signature.trim());
        } catch (Exception e) {
            return false;
        }
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

    private static Map<String,String> parseForm(String body) throws UnsupportedEncodingException {
        Map<String,String> map = new HashMap<>();
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
        ex.getResponseBody().write(out);
        ex.close();
    }
}