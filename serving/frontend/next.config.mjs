/** @type {import('next').NextConfig} */
const BACKEND = process.env.BACKEND_URL ?? "http://webapp-backend:8000";

export default {
  output: "standalone",
  reactStrictMode: true,
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${BACKEND}/api/:path*` }];
  },
};
