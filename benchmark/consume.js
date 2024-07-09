import http from 'k6/http';

export default function () {
  http.get('http://localhost:8080/topics/cart/subscribers/counter?timeout=5s');
}