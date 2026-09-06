import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: 25,
  duration: "30s",
  maxRedirects: 0,
};

export default function () {
  const response = http.get("http://localhost:5287/24dbdd");

  check(response, {
    "status is 302": (r) => r.status === 302,
  });
}
