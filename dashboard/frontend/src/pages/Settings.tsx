import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useAuth } from "@/lib/auth";
import { api, ApiError } from "@/lib/api";

type RetentionItem = { collection: string; ttl_days: number; reason: string };
type CollectionStat = { name: string; estimated_count: number | null };

export function SettingsPage() {
  const { user } = useAuth();
  const [retention, setRetention] = useState<RetentionItem[] | null>(null);
  const [system, setSystem] = useState<{ db_name: string; collections: CollectionStat[] } | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const [r, s] = await Promise.all([
          api.get<{ items: RetentionItem[] }>("/api/settings/retention"),
          api.get<{ db_name: string; collections: CollectionStat[] }>("/api/settings/system"),
        ]);
        if (!cancelled) { setRetention(r.items); setSystem(s); setError(null); }
      } catch (err) {
        if (!cancelled) setError(err instanceof ApiError ? err.message : "failed to load settings");
      }
    };
    load();
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold tracking-tight">Settings</h2>
        <p className="text-sm text-muted-foreground">User profile, retention policies, system health</p>
      </div>

      {error && (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Profile</CardTitle>
        </CardHeader>
        <CardContent className="space-y-1 text-sm">
          <div>Username: <span className="font-medium">{user?.username}</span></div>
          <div>Role: <span className="font-medium">{user?.role}</span></div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>GDPR retention policies</CardTitle>
          <CardDescription>TTL indexes enforced at the database layer (see ethics/GDPR.md)</CardDescription>
        </CardHeader>
        <CardContent>
          {!retention ? (
            <p className="text-sm text-muted-foreground">Loading…</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Collection</TableHead>
                  <TableHead>TTL (days)</TableHead>
                  <TableHead>Reason</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {retention.map(r => (
                  <TableRow key={r.collection}>
                    <TableCell className="font-mono text-xs">{r.collection}</TableCell>
                    <TableCell>{r.ttl_days}</TableCell>
                    <TableCell className="text-xs">{r.reason}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>System status</CardTitle>
          <CardDescription>Collections in <code>{system?.db_name || "energy_db"}</code></CardDescription>
        </CardHeader>
        <CardContent>
          {!system ? (
            <p className="text-sm text-muted-foreground">Loading…</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Collection</TableHead>
                  <TableHead>Estimated docs</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {system.collections.map(c => (
                  <TableRow key={c.name}>
                    <TableCell className="font-mono text-xs">{c.name}</TableCell>
                    <TableCell>{c.estimated_count ?? "—"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
