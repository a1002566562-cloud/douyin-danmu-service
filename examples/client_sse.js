// 最小浏览器 / Node(18+) 接入示例：SSE 订阅弹幕事件。
// 浏览器：直接 <script src="client_sse.js"></script>；Node: node client_sse.js
(function () {
  const url = "http://127.0.0.1:8520/stream";
  const es = new EventSource(url);
  es.onmessage = (m) => {
    const ev = JSON.parse(m.data);
    switch (ev.type) {
      case "chat":
        console.log(`[弹幕] ${ev.user}: ${ev.content}`, ev.avatar);
        break;
      case "gift":
        console.log(`[礼物] ${ev.user} 送 ${ev.gift} x${ev.count}`);
        break;
      case "like":
        console.log(`[点赞] ${ev.user} x${ev.count}`);
        break;
      case "enter":
      case "follow":
        console.log(`[${ev.type === "enter" ? "进场" : "关注"}] ${ev.user}`);
        break;
      default:
        console.log(`[${ev.type}]`, ev);
    }
  };
  es.onerror = () => console.warn("SSE 断开，EventSource 会自动重连");
})();

// Node 环境兜底
if (typeof window === "undefined" && typeof EventSource === "undefined") {
  console.error("Node 环境请改用 client_ws.py 或 npm 的 eventsource 包");
}
