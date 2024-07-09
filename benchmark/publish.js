import http from 'k6/http';

export default function() {
    const url = 'http://localhost:8080/topics/cart/messages'
    const payload = JSON.stringify({
        message: 'test publishing message'
    })

    const params = {
        headers: {
            'Content-Type': 'application/json'
        }
    }

    http.post(url, payload,  params)
}