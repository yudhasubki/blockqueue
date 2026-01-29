import http from 'k6/http';
import { check } from 'k6';

export default function () {
    const url = 'http://localhost:8090/topics/cart/messages'
    const payload = JSON.stringify({
        message: 'test publishing message'
    })

    const params = {
        headers: {
            'Content-Type': 'application/json'
        }
    }

    const res = http.post(url, payload, params)

    check(res, {
        'status is 2xx': (r) => r.status >= 200 && r.status < 300,
    })
}