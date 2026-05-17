import { Navigate, Outlet, useLocation } from "react-router-dom";
import { useAuth } from "@/lib/auth";

export function ProtectedRoute() {
  const { user, loading } = useAuth();
  const location = useLocation();

  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center text-muted-foreground">
        Loading session…
      </div>
    );
  }
  if (!user) return <Navigate to="/login" state={{ from: location }} replace />;
  return <Outlet />;
}
