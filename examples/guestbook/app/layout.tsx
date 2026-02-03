export const metadata = {
  title: 'Turso + Vercel Example',
  description: 'A guestbook app using Turso on Vercel',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body style={{ fontFamily: 'system-ui, sans-serif', maxWidth: '600px', margin: '0 auto', padding: '2rem' }}>
        {children}
      </body>
    </html>
  );
}
