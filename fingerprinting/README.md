# Как использовать механизм fingerprint в Majorka

1. Допустим у нас есть трекинг линка: http://4096.io/campaign/g3m?zone={zoneid}&cost={cost}&currency=usd&connection_type={connection.type}&carrier={carrier}&clickid=${SUBID}
2. Мы хотим добавить фингерпринтинг для этой трекинг линки
3. Перенастраиваем рекламную кампанию в рекламной сети так, чтобы она вела не на исходную линку (п. 1) а на вот такую http://4096.io/f/?redir=http://4096.io/campaign/g3m?zone={zoneid}&cost={cost}&currency=usd&connection_type={connection.type}&carrier={carrier}&clickid=${SUBID} То есть добавлен префикс http://4096.io/f/?redir=

Префикс `http://4096.io/f/?redir=` добавляет еще один редирект, который пробрасывает параметры в исходную трекинг линку.
Если фингерпринт удалось получить, поля sub_id_10..sub_id_13 заполнены данными фингерпринта.
Если фингерпринт *не удалось получить*, поле sub_id_14 содержит сообщение о произошедшей ошибке JS.

# Как работает?

Файл `fingerprint2.min.js` это минифицированный вариант библиотеки Валентина Васильева (valentin.vasilyev@outlook.com)
Её исходный код доступен по ссылке [здесь](https://github.com/valve/fingerprintjs2)

Эту библиотеку следует держать в CDN.

Файл `index.original.html` находится на том же сервере что и majorka, доступен по тому же домену, чтобы
исключить повторный DNS лукап при обращении к библиотеке и редиректу от majorka.

index.html, который на целевом сервере доступен по УРЛу `/f/` вычисляет
фингерпринт и осуществляет редирект по ссылке переданной в параметре URL `redir`,
передавая туда же через поля `sub_id_10..sub_id_13` данные фингерпринта.

В силу того, что `majorka` умеет сохранять поля `sub_id_*`, данные сохраняются для хита,
однако надо понимать, что Referer подменяется, т.к. нет возможности средствами браузера его пробросить.

# Конфигурация nginx
```
server {
	listen 80;
	listen [::]:80;

	server_name 4096.io;

        location /f {
           alias /var/www/fingerprint;
           index index.html;
        }

        location / {
            access_log /var/log/nginx/redirect/access.log;
            error_log /var/log/nginx/redirect/error.log warn;

            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

            proxy_pass http://127.0.0.1:8000/;
        }
}

```
