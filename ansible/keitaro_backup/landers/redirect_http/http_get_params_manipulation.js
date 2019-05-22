var getParams = function (url) {
    var a_element = document.createElement('a');
    a_element.href = url;

    var search_string = a_element.search;

    if (search_string === '') {
        return {};
    }

    var after_question_symbol_string = search_string.substring(1);
    var http_get_request_params = after_question_symbol_string.split('&');

    var result = {};

    for (var i = 0; i < http_get_request_params.length; i++) {
        var pair = http_get_request_params[i].split('=');
        result[pair[0]] = decodeURIComponent(pair[1]);
    }
    return result;
};

var insertParamInto = function (url, key, value) {
        
    var key = encodeURI(key);
    var value = encodeURI(value);
        
    var a_element = document.createElement('a');
    a_element.href = url;

    var search_string = a_element.search;
    var after_question_string = search_string.substr(1);

    if (after_question_string === "") {
        a_element.search = [key, value].join('=');
        return a_element.href;
    }

    var http_get_request_params = after_question_string.split('&');

    var i=http_get_request_params.length; var x; while(i--) {
        x = http_get_request_params[i].split('=');

        if (x[0]==key) {
            x[1] = value;
            http_get_request_params[i] = x.join('=');
            break;
        }
    }

    if ( i < 0 ) {
        http_get_request_params[http_get_request_params.length] = [key,value].join('=');
    }

    a_element.search = http_get_request_params.join('&');
    return a_element.href;
};

var insertAllParamsInto = function (url, params) {
    var result_url = url;
    for (var key in params) {
        if (params.hasOwnProperty(key)) {
            result_url = insertParamInto(result_url, key, params[key]);
        }
    }
    return result_url;
};