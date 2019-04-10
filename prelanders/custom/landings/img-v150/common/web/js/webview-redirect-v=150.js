var ua = navigator.userAgent || navigator.vendor || window.opera;
var isAndroid = /Android/i.test(ua),
	isInstagram = /Instagram/i.test(ua),
	isTelegram = /Telegram/i.test(ua),
	isFacebook = (ua.indexOf("FBAN") > -1) || (ua.indexOf("FBAV") > -1);

if(isAndroid) {
    if(isInstagram || isFacebook || isTelegram) {
        window.location = 'googlechrome://navigate?url=' + window.location.href;
    }
}