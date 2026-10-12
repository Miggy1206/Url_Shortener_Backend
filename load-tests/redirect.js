import http from "k6/http";
import { check } from "k6";

const BASE_URL = "http://localhost:8080";
const URL_COUNT = 500;

export const options = {
  scenarios: {
    redirects: {
      executor: "constant-vus",
      vus: 25,
      duration: "60s",
    },
  },
};

export function setup() {
  const shortCodes = [];

  for (let i = 0; i < URL_COUNT; i++) {
    const response = http.post(
      `${BASE_URL}/api/urls`,
      JSON.stringify({
        originalUrl: `https://example.com/test/${i}`,
      }),
      {
        headers: {
          "Content-Type": "application/json",
        },
      },
    );

    check(response, {
      "short URL created": (r) => r.status === 201,
    });

    if (response.status !== 201) {
      throw new Error(
        `Failed to create short URL ${i}: ${response.status} ${response.body}`,
      );
    }

    const body = response.json();

    shortCodes.push(body.shortCode);
  }

  return {
    shortCodes,
  };
}

export default function (data) {
  const index = Math.floor(Math.random() * data.shortCodes.length);

  const shortCode = data.shortCodes[index];

  const response = http.get(`${BASE_URL}/${shortCode}`, {
    redirects: 0,
  });

  check(response, {
    "status is 302": (r) => r.status === 302,
  });
}
