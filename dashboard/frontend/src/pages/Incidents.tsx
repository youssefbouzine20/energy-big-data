import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { api, ApiError } from "@/lib/api";
import { formatTime } from "@/lib/utils";

type Incident = {
  incident_id: string;
  timestamp: string;
  zone: string;
  severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
  type: string;
  description: string;
  resolved: boolean;
};
type Word = { text: string; value: number };

const severityVariant: Record<Incident["severity"], "success" | "secondary" | "warning" | "destructive"> = {
  LOW: "secondary", MEDIUM: "warning", HIGH: "destructive", CRITICAL: "destructive",
};

export function IncidentsPage() {
  const [incidents, setIncidents] = useState<Incident[] | null>(null);
  const [words, setWords] = useState<Word[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const [i, w] = await Promise.all([
          api.get<{ items: Incident[] }>("/api/incidents?limit=25"),
          api.get<{ items: Word[] }>("/api/incidents/wordcloud?days=7"),
        ]);
        if (!cancelled) { setIncidents(i.items); setWords(w.items); setError(null); }
      } catch (err) {
        if (!cancelled) setError(err instanceof ApiError ? err.message : "failed to load incidents");
      }
    };
    load();
    const id = setInterval(load, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const maxFreq = words?.[0]?.value || 1;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold tracking-tight">Incidents</h2>
        <p className="text-sm text-muted-foreground">Last 25 events · NLP keyword cloud over 7 days</p>
      </div>

      {error && (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Keyword cloud</CardTitle>
          <CardDescription>Most frequent terms from incident descriptions and NLP keywords</CardDescription>
        </CardHeader>
        <CardContent>
          {!words || words.length === 0 ? (
            <p className="py-6 text-sm text-muted-foreground">No incidents in the last 7 days.</p>
          ) : (
            <div className="flex flex-wrap gap-x-3 gap-y-2 leading-snug">
              {words.map((w, i) => {
                const size = 0.8 + (w.value / maxFreq) * 1.6;
                const hue = (i * 37) % 360;
                return (
                  <span
                    key={w.text}
                    className="font-semibold"
                    style={{ fontSize: `${size}rem`, color: `hsl(${hue}, 70%, 45%)` }}
                    title={`${w.text}: ${w.value}`}
                  >
                    {w.text}
                  </span>
                );
              })}
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Recent incidents</CardTitle>
        </CardHeader>
        <CardContent>
          {!incidents ? (
            <p className="text-sm text-muted-foreground">Loading…</p>
          ) : incidents.length === 0 ? (
            <p className="text-sm text-muted-foreground">No incidents recorded yet.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Time</TableHead>
                  <TableHead>Zone</TableHead>
                  <TableHead>Severity</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Description</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {incidents.map(i => (
                  <TableRow key={i.incident_id}>
                    <TableCell className="whitespace-nowrap text-xs">{formatTime(i.timestamp)}</TableCell>
                    <TableCell>{i.zone}</TableCell>
                    <TableCell><Badge variant={severityVariant[i.severity]}>{i.severity}</Badge></TableCell>
                    <TableCell className="text-xs">{i.type}</TableCell>
                    <TableCell className="max-w-md truncate text-xs">{i.description}</TableCell>
                    <TableCell>
                      <Badge variant={i.resolved ? "success" : "outline"}>{i.resolved ? "resolved" : "open"}</Badge>
                    </TableCell>
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
