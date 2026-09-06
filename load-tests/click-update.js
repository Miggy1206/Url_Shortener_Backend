import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: 25,
  duration: "30s",
};

export default function () {
  const response = http.post(
    "http://localhost:5287/benchmark/increment/24dbdd",
  );

  check(response, {
    "status is 200": (r) => r.status === 200,
  });
}
