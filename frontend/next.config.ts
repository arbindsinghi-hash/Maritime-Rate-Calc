import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Proxy /api requests to the FastAPI backend
  async rewrites() {
    const backend = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
    return [
      {
        source: "/api/:path*",
        destination: `${backend}/api/:path*`,
      },
    ];
  },
  // react-pdf uses canvas — skip server-side bundling
  turbopack: {
    resolveAlias: {
      canvas: { browser: "" },
    },
  },
};

export default nextConfig;
