import { NavLink } from "react-router-dom";
import {
  LayoutDashboard, MapPin, LineChart, AlertTriangle,
  BellRing, ShieldCheck, Settings, Zap,
} from "lucide-react";
import { cn } from "@/lib/utils";

const links = [
  { to: "/", label: "Overview", icon: LayoutDashboard, end: true },
  { to: "/heatmap", label: "Heatmap", icon: MapPin },
  { to: "/predictions", label: "Predictions", icon: LineChart },
  { to: "/incidents", label: "Incidents", icon: AlertTriangle },
  { to: "/alerts", label: "Alerts", icon: BellRing },
  { to: "/quality", label: "Data Quality", icon: ShieldCheck },
  { to: "/settings", label: "Settings", icon: Settings },
];

export function Sidebar() {
  return (
    <aside className="hidden md:flex md:w-64 md:flex-col border-r bg-card">
      <div className="flex h-16 items-center gap-2 border-b px-6">
        <Zap className="h-6 w-6 text-primary" />
        <div className="flex flex-col leading-tight">
          <span className="font-semibold">Energy Grid</span>
          <span className="text-xs text-muted-foreground">Tetouan Monitor</span>
        </div>
      </div>
      <nav className="flex-1 space-y-1 p-4">
        {links.map(({ to, label, icon: Icon, end }) => (
          <NavLink
            key={to}
            to={to}
            end={end}
            className={({ isActive }) =>
              cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                isActive
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:bg-accent hover:text-accent-foreground"
              )
            }
          >
            <Icon className="h-4 w-4" />
            {label}
          </NavLink>
        ))}
      </nav>
      <div className="border-t p-4 text-xs text-muted-foreground">
        M126 Big Data · ENSA Tétouan
      </div>
    </aside>
  );
}
