import { BrowserRouter, Route, Routes } from "react-router-dom";
import { AuthProvider } from "@/lib/auth";
import { ProtectedRoute } from "@/components/ProtectedRoute";
import { Layout } from "@/components/Layout";
import { LoginPage } from "@/pages/Login";
import { OverviewPage } from "@/pages/Overview";
import { HeatmapPage } from "@/pages/Heatmap";
import { PredictionsPage } from "@/pages/Predictions";
import { IncidentsPage } from "@/pages/Incidents";
import { AlertsPage } from "@/pages/Alerts";
import { QualityPage } from "@/pages/Quality";
import { SettingsPage } from "@/pages/Settings";

export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route element={<ProtectedRoute />}>
            <Route element={<Layout />}>
              <Route index element={<OverviewPage />} />
              <Route path="/heatmap" element={<HeatmapPage />} />
              <Route path="/predictions" element={<PredictionsPage />} />
              <Route path="/incidents" element={<IncidentsPage />} />
              <Route path="/alerts" element={<AlertsPage />} />
              <Route path="/quality" element={<QualityPage />} />
              <Route path="/settings" element={<SettingsPage />} />
            </Route>
          </Route>
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  );
}
