import type { Metadata } from 'next';
import './globals.css';
export const metadata: Metadata = { title: 'Backtest Desk', description: 'Upstox candle archive and strategy research' };
export default function RootLayout({ children }: Readonly<{children: React.ReactNode}>) { return <html lang="en"><body>{children}</body></html>; }
