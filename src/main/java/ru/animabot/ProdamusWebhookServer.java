package ru.animabot;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Усиленный HTTP-сервер для приёма webhook от Prodamus.
 * Поддерживает:
 *  - POST /webhook/prodamus
 *  - application/x-www-form-urlencoded И application/json
 *  - подпись HMAC-SHA256 в разных заголовках
 *  - извлечение order_id / customer_extra / days из разных мест
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
            long started = System.currentTimeMillis();
            try {
                if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                    respond(ex, 405, "method not allowed"); return;
                }

                byte[] raw = readAll(ex.getRequestBody());
                String ctype = firstHeader(ex, "Content-Type");
                String body  = new String(raw, StandardCharsets.UTF_8);

                // Подпись: пробуем несколько заголовков
                String gotSig = firstNonEmpty(
                        firstHeader(ex, "Signature"),
                        firstHeader(ex, "X-Signature"),
                        firstHeader(ex, "X-Webhook-Signature"),
                        firstHeader(ex, "X-Auth-Signature"),
                        null
                );

                // Логируем, чтобы понять, что реально присылает Prodamus
                LOG.info("[prodamus] incoming: ct='{}', sigHeader='{}', rawLen={} bytes",
                        ctype, gotSig != null ? "present" : "absent", raw.length);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[prodamus] headers: {}", headersToMap(ex));
                    LOG.debug("[prodamus] body: {}", body);
                }

                // Проверка подписи (если секрет задан)
                if (!secret.isBlank()) {
                    if (!verifySignature(raw, gotSig, secret)) {
                        LOG.warn("[prodamus] bad signature -> reject");
                        respond(ex, 400, "bad signature");
                        return;
                    }
                } else {
                    LOG.warn("[prodamus] PRODAMUS_SECRET is empty — signature check is DISABLED!");
                }

                // Парсим тело в Map<String,Object> (json или form)
                Map<String, Object> any = parseBodyToMap(ctype, body);
                Map<String, String> flat = flattenMap(any);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("[prodamus] flat map: {}", flat);
                }

                // Пытаемся достать UID (order_id / customer_extra / альтернативы)
                long uid = tryParseLongAny(flat,
                        "order_id", "orderId", "orderid",
                        "customer_extra", "customerExtra",
                        "order.customer_extra", "invoice.customer_extra",
                        "customer.extra", "customer.extra_id",
                        "meta.user_id", "meta.uid"
                );

                // Пытаемся достать days
                long daysL = tryParseLongAny(flat,
                        "days", "period", "period_days",
                        "meta.days", "meta.period_days",
                        "order.days", "invoice.days"
                );
                int days = (daysL > 0 && daysL <= Integer.MAX_VALUE) ? (int) daysL : 30; // дефолт 30

                if (uid <= 0) {
                    // Иногда используют "custom"/"comment"
                    uid = tryParseLongAny(flat, "custom", "comment", "note");
                }

                LOG.info("[prodamus] parsed: uid={}, days={}", uid, days);

                if (uid > 0) {
                    bot.onProdamusPaid(uid, days);
                    respond(ex, 200, "ok");
                } else {
                    LOG.warn("[prodamus] webhook without uid; flat={}", flat);
                    // Всё равно отвечаем 200, чтобы Prodamus не ретраил.
                    respond(ex, 200, "ok (no uid)");
                }

            } catch (Exception e) {
                LOG.error("[prodamus] webhook error", e);
                respond(ex, 500, "internal error");
            } finally {
                LOG.info("[prodamus] handled in {} ms", (System.currentTimeMillis() - started));
            }
        }
    }

    // ===== verify signature =====

    private static boolean verifySignature(byte[] rawBody, String signature, String secret) {
        if (signature == null) return false;
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
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

    private static String firstHeader(HttpExchange ex, String name) {
        try { return ex.getRequestHeaders().getFirst(name); } catch (Exception ignore) { return null; }
    }

    private static Map<String, List<String>> headersToMap(HttpExchange ex) {
        Map<String, List<String>> out = new LinkedHashMap<>();
        try {
            ex.getRequestHeaders().forEach((k,v) -> out.put(k, new ArrayList<>(v)));
        } catch (Exception ignore) {}
        return out;
    }

    // ===== parsing =====

    private static Map<String, Object> parseBodyToMap(String contentType, String body) {
        if (body == null || body.isBlank()) return new LinkedHashMap<>();
        String ct = (contentType == null) ? "" : contentType.toLowerCase(Locale.ROOT);

        // JSON?
        if (ct.contains("json") || body.trim().startsWith("{") || body.trim().startsWith("[")) {
            try {
                return JsonLike.parse(body);
            } catch (Exception e) {
                // fallback в form
                return parseForm(body);
            }
        }
        // form-urlencoded
        return parseForm(body);
    }

    /** form → Map<String,Object> (исправлено: раньше возвращал Map<String,String>) */
    private static Map<String,Object> parseForm(String body) {
        Map<String,Object> map = new LinkedHashMap<>();
        try {
            if (body == null || body.isBlank()) return map;
            for (String pair : body.split("&")) {
                int i = pair.indexOf('=');
                if (i < 0) continue;
                String k = URLDecoder.decode(pair.substring(0, i), StandardCharsets.UTF_8);
                String v = URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8);
                map.put(k, v);
            }
        } catch (Exception e) {
            // ignore
        }
        return map;
    }

    /** Плоский словарь: разворачиваем вложенные структуры в ключи вида a.b.c */
    @SuppressWarnings("unchecked")
    private static Map<String,String> flattenMap(Map<String, Object> any) {
        Map<String,String> out = new LinkedHashMap<>();
        if (any == null) return out;
        Deque<String> path = new ArrayDeque<>();
        flatten(any, path, out);
        return out;
    }

    @SuppressWarnings("unchecked")
    private static void flatten(Object node, Deque<String> path, Map<String,String> out) {
        if (node == null) return;
        if (node instanceof Map<?,?> m) {
            for (Map.Entry<?,?> e : m.entrySet()) {
                String key = String.valueOf(e.getKey());
                path.addLast(key);
                flatten(e.getValue(), path, out);
                path.removeLast();
            }
        } else if (node instanceof List<?> list) {
            for (int i=0;i<list.size();i++) {
                path.addLast(String.valueOf(i));
                flatten(list.get(i), path, out);
                path.removeLast();
            }
        } else {
            String k = String.join(".", path);
            out.put(k, String.valueOf(node));
        }
    }

    private static long tryParseLongAny(Map<String,String> map, String... keys) {
        for (String k : keys) {
            String v = map.get(k);
            if (v != null && !v.isBlank()) {
                try { return Long.parseLong(v.trim()); } catch (Exception ignore) {}
            }
        }
        return -1L;
    }

    // ===== io utils =====

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

    private static String firstNonEmpty(String... v) {
        if (v == null) return null;
        for (String s : v) if (s != null && !s.isBlank()) return s;
        return null;
    }

    // ===== мини JSON-парсер (без внешних зависимостей) =====

    /** Очень простая реализация, рассчитанная на обычные плоские объекты с вложенными map/list/строками/числами/boolean. */
    static class JsonLike {
        @SuppressWarnings("unchecked")
        static Map<String,Object> parse(String s) throws Exception {
            return (Map<String,Object>) new Parser(s).parseValue();
        }

        private static class Parser {
            private final String str;
            private int i = 0;
            Parser(String s) { this.str = s; }

            Object parseValue() throws Exception {
                skipWs();
                if (i >= str.length()) return null;
                char c = str.charAt(i);
                if (c == '{') return parseObject();
                if (c == '[') return parseArray();
                if (c == '"' || c=='\'') return parseString();
                if (isDigit(c) || c=='-' ) return parseNumber();
                if (str.startsWith("true", i))  { i+=4; return Boolean.TRUE; }
                if (str.startsWith("false", i)) { i+=5; return Boolean.FALSE; }
                if (str.startsWith("null", i))  { i+=4; return null; }
                return parseBareWord();
            }

            Map<String,Object> parseObject() throws Exception {
                Map<String,Object> m = new LinkedHashMap<>();
                expect('{'); skipWs();
                if (peek() == '}') { i++; return m; }
                while (true) {
                    skipWs();
                    String key = String.valueOf(parseValue());
                    skipWs(); expect(':');
                    Object val = parseValue();
                    m.put(trimQuotes(key), val);
                    skipWs();
                    char c = peek();
                    if (c == ',') { i++; continue; }
                    if (c == '}') { i++; break; }
                    if (i >= str.length()) break;
                }
                return m;
            }

            List<Object> parseArray() throws Exception {
                List<Object> a = new ArrayList<>();
                expect('['); skipWs();
                if (peek() == ']') { i++; return a; }
                while (true) {
                    Object v = parseValue();
                    a.add(v);
                    skipWs();
                    char c = peek();
                    if (c == ',') { i++; continue; }
                    if (c == ']') { i++; break; }
                    if (i >= str.length()) break;
                }
                return a;
            }

            String parseString() {
                char quote = str.charAt(i++);
                StringBuilder sb = new StringBuilder();
                while (i < str.length()) {
                    char c = str.charAt(i++);
                    if (c == quote) break;
                    if (c == '\\' && i < str.length()) {
                        char n = str.charAt(i++);
                        switch (n) {
                            case 'n': sb.append('\n'); break;
                            case 'r': sb.append('\r'); break;
                            case 't': sb.append('\t'); break;
                            case '"': sb.append('"');  break;
                            case '\'':sb.append('\''); break;
                            case '\\':sb.append('\\'); break;
                            case 'u':
                                if (i+3 < str.length()) {
                                    String hex = str.substring(i, i+4);
                                    try { sb.append((char) Integer.parseInt(hex,16)); } catch (Exception ignore) {}
                                    i += 4;
                                }
                                break;
                            default: sb.append(n);
                        }
                    } else {
                        sb.append(c);
                    }
                }
                return sb.toString();
            }

            Number parseNumber() {
                int j = i;
                while (i < str.length()) {
                    char c = str.charAt(i);
                    if (isDigit(c) || c=='-' || c=='+' || c=='.' || c=='e' || c=='E') i++;
                    else break;
                }
                String sub = str.substring(j, i);
                try {
                    if (sub.contains(".") || sub.contains("e") || sub.contains("E")) return Double.parseDouble(sub);
                    long l = Long.parseLong(sub);
                    if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) return (int) l;
                    return l;
                } catch (Exception e) {
                    return 0;
                }
            }

            String parseBareWord() {
                int j = i;
                while (i < str.length()) {
                    char c = str.charAt(i);
                    if (Character.isLetterOrDigit(c) || c=='_' || c=='.' || c=='-') i++;
                    else break;
                }
                return str.substring(j, i);
            }

            void skipWs() {
                while (i < str.length()) {
                    char c = str.charAt(i);
                    if (c==' '||c=='\n'||c=='\r'||c=='\t') i++;
                    else break;
                }
            }
            void expect(char ch) throws Exception {
                skipWs();
                if (i >= str.length() || str.charAt(i) != ch) throw new Exception("expected '"+ch+"'");
                i++;
            }
            char peek() { return (i < str.length()) ? str.charAt(i) : '\0'; }
            boolean isDigit(char c) { return c>='0' && c<='9'; }
            String trimQuotes(String s) {
                if (s == null || s.length()<2) return s;
                if ((s.charAt(0)=='"' && s.charAt(s.length()-1)=='"') ||
                        (s.charAt(0)=='\'' && s.charAt(s.length()-1)=='\'')) {
                    return s.substring(1, s.length()-1);
                }
                return s;
            }
        }
    }
}