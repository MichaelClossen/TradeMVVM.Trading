// background.js - receives messages from content script and forwards via UDP

let socketId = null;
const PORT = 54123;
const HOST = '127.0.0.1';

function supportsUdp() {
  try {
    if (typeof browser !== 'undefined' && browser.sockets && browser.sockets.udp) return true;
    if (typeof chrome !== 'undefined' && chrome.sockets && chrome.sockets.udp) return true;
  } catch (e) { }
  return false;
}

function createSocket() {
  try {
    if (typeof browser !== 'undefined' && browser.sockets && browser.sockets.udp) {
      browser.sockets.udp.create({}, function(createInfo) {
        socketId = createInfo.socketId;
        try { browser.sockets.udp.bind(socketId, HOST, 0, function(result) {}); } catch(e) { console.warn('bind udp failed', e); }
      });
    }
    else if (typeof chrome !== 'undefined' && chrome.sockets && chrome.sockets.udp) {
      chrome.sockets.udp.create({}, function(createInfo) {
        socketId = createInfo.socketId;
        try { chrome.sockets.udp.bind(socketId, HOST, 0, function(result) {}); } catch(e) { console.warn('bind udp failed', e); }
      });
    }
  }
  catch (e) { console.warn('createSocket error', e); }
}

function sendViaUdp(text) {
  try {
    const data = new TextEncoder().encode(text || '');
    if (typeof browser !== 'undefined' && browser.sockets && browser.sockets.udp) {
      if (!socketId) createSocket();
      try { browser.sockets.udp.send(socketId, data, HOST, PORT, function(sendInfo) {}); } catch(e) { console.warn('udp send failed', e); }
      return true;
    }
    if (typeof chrome !== 'undefined' && chrome.sockets && chrome.sockets.udp) {
      if (!socketId) createSocket();
      try { chrome.sockets.udp.send(socketId, data, HOST, PORT, function(sendInfo) {}); } catch(e) { console.warn('udp send failed', e); }
      return true;
    }
  } catch (e) { console.warn('sendViaUdp error', e); }
  return false;
}

function sendViaHttp(text) {
  try {
    fetch('http://127.0.0.1:54123/notify', { method: 'POST', headers: { 'Content-Type': 'text/plain; charset=utf-8' }, body: text })
      .catch(e => console.warn('http fallback failed', e));
  } catch (e) { console.warn('sendViaHttp error', e); }
}

// small logger to help debugging during development
function dbg(...args) { try { console.log.apply(console, args); } catch(e){} }

dbg('background.js initialized. supportsUdp=', supportsUdp());

// initialize socket only if supported
if (supportsUdp()) createSocket();

browser.runtime.onMessage.addListener(function(message, sender) {
  try {
    dbg('background.onMessage received', message, 'from', sender);
    const text = message && message.text ? message.text : '';
    // try UDP first when available, otherwise fallback to HTTP POST
    const usedUdp = supportsUdp() && sendViaUdp(text);
    if (usedUdp) dbg('dispatched via UDP'); else { dbg('using HTTP fallback'); sendViaHttp(text); }
  } catch (e) { console.warn('onMessage handler error', e); }
});
