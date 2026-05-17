import { useEffect, useState } from "react";
import { LogOut, User } from "lucide-react";
import { useAuth } from "@/lib/auth";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";

export function Header() {
  const { user, logout } = useAuth();
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(id);
  }, []);

  return (
    <header className="flex h-16 items-center justify-between border-b bg-card px-6">
      <div>
        <h1 className="text-lg font-semibold">Real-time Energy Monitor</h1>
        <p className="text-xs text-muted-foreground">
          {now.toLocaleString(undefined, { dateStyle: "full", timeStyle: "medium" })}
        </p>
      </div>
      <div className="flex items-center gap-3">
        {user && (
          <>
            <Badge variant={user.role === "admin" ? "default" : "secondary"}>{user.role}</Badge>
            <div className="flex items-center gap-2 text-sm">
              <User className="h-4 w-4 text-muted-foreground" />
              <span>{user.username}</span>
            </div>
            <Button variant="ghost" size="sm" onClick={() => logout()}>
              <LogOut className="mr-2 h-4 w-4" /> Logout
            </Button>
          </>
        )}
      </div>
    </header>
  );
}
