// content-script.js - example to capture Notification API calls and forward to background

(function() {
  try {
    // Inject a small script into the page context so we can override both
    // the Notification constructor and ServiceWorkerRegistration.prototype.showNotification
    // Many sites trigger notifications from a service worker; overriding in page scope
    // lets us catch calls made from page scripts as well as registration-based calls.
    const injected = document.createElement('script');
    injected.type = 'text/javascript';
    injected.textContent = `
      (function() {
        try {
          const OrigNotification = window.Notification;
          function InterceptNotification(title, options) {
            try {
              window.postMessage({ direction: 'toExtension', method: 'Notification', title: title, options: options || {} }, '*');
            } catch(e) {}
            return OrigNotification ? new OrigNotification(title, options) : null;
          }
          try { InterceptNotification.requestPermission = OrigNotification.requestPermission.bind(OrigNotification); } catch(e) {}
          try { InterceptNotification.permission = OrigNotification.permission; } catch(e) {}
          window.Notification = InterceptNotification;

          // override registration.showNotification when called from page context
          try {
            const swProto = ServiceWorkerRegistration && ServiceWorkerRegistration.prototype;
            if (swProto && !swProto.__intercepted) {
              const origShow = swProto.showNotification;
              swProto.showNotification = function(title, options) {
                try { window.postMessage({ direction: 'toExtension', method: 'showNotification', title: title, options: options || {} }, '*'); } catch(e) {}
                return origShow.apply(this, arguments);
              };
              swProto.__intercepted = true;
            }
          } catch(e) {}
        } catch(e) {}
      })();`;
    (document.head || document.documentElement).appendChild(injected);
    injected.parentNode.removeChild(injected);

    // listen for events posted from the injected page script and forward to background
    window.addEventListener('message', function(ev) {
      try {
        if (!ev.data || ev.data.direction !== 'toExtension') return;
        const payload = ev.data;
        const title = payload.title || '';
        const body = (payload.options && payload.options.body) ? payload.options.body : '';
        const tag = (payload.options && payload.options.tag) ? payload.options.tag : '';
        const url = window.location.href;
        const txt = JSON.stringify({ method: payload.method || 'unknown', title: title, body: body, tag: tag, url: url, ts: new Date().toISOString() });
        try { browser.runtime.sendMessage({ text: txt }); } catch(e) { try { chrome.runtime.sendMessage({ text: txt }); } catch(e) {} }
      } catch(e) {}
    }, false);
  } catch(e) {}
})();
