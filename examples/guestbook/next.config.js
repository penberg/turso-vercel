/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    serverComponentsExternalPackages: [
      '@tursodatabase/vercel-experimental',
      '@tursodatabase/sync',
      '@tursodatabase/sync-common',
      '@tursodatabase/database-common',
      '@aspect-build/better-sqlite3',
    ],
  },
};

module.exports = nextConfig;
