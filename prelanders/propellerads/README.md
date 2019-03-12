## Площадка #2483917 (googlecomservice.com)
1. Файл djghajhg64567476q.js - это service worker. Его следует положить в корень сайта
2. Код сбора подписок - сразу после ```<body>```:
```html
<script>(function() {
  function getParameterByName(name, url) {
    if (!url) url = window.location.href;
    name = name.replace(/[\[\]]/g, '\\$&');
    var regex = new RegExp('[?&]' + name + '(=([^&#]*)|&|#|$)'),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, ' '));
  }

  var loc = document.location;
  var tb = function(){ loc.href = ['ht', 'tps:/', '/blud', 'wan.com/a', 'fu.ph', 'p?zon', 'eid=2483917'].join(''); };
  var bc = function(){ loc.href = ['ht', 'tps:', '/', '/go', 'ogle.com/'].join(''); };

  var url = new URL(window.location.href);
  var pci = getParameterByName('external_id', window.location.href);
  var ppi = getParameterByName('placementid', window.location.href);
  var tag = document.createElement('script');
  tag.type = 'text/javascript';
  tag.dataset['sdk'] = 'sdk';
  tag.src = '//pushokey.com/ntfc.php?p=2483903&ucis=true&m=https&nbinp=true' + '&var='+ ppi + '&ymid=' + pci;

  function setupHandlers() {
    // sdk.onBeforePermissionPrompt();
    sdk.onPermissionDefault(tb);
    sdk.onPermissionAllowed(tb);
    sdk.onPermissionDenied(tb);
    sdk.onAlreadySubscribed(tb);
  }

  tag.onload = setupHandlers;

  document.head.appendChild(tag);
  })();</script>
```
3. Документация по SDK:
```js
SDK Reference:
sdk.onBeforePermissionPrompt(function() { }); // this code will be executed before push tag will ask for permission
sdk.onPermissionDefault(function() { }); // this code will be executed if user skips notification permission dialog
sdk.onPermissionAllowed(function() { }); // this code will be executed if user clicks on Allow button
sdk.onPermissionDenied(function() { }); // this code will be executed if user clicks on Deny button
sdk.onAlreadySubscribed(function() { }); // this code will be executed if user has been subscribed to notifications already
```
4. При закупе в блек добавляем 2483917 и 1407888 (это технический источник, сюда попадает разный шлак и трафбек у Пропеллера(!))
5. Cloudfront CDN: 	d17yzektsp7kjw.cloudfront.net / distribution id: ESMQISTA8EELO  
