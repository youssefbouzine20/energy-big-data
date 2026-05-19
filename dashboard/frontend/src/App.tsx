import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './lib/auth';
import { ProtectedRoute } from './components/ProtectedRoute';
import { Layout } from './components/Layout';
import { Login } from './pages/Login';
import { Overview } from './pages/Overview';
import { Heatmap } from './pages/Heatmap';
import { Predictions } from './pages/Predictions';
import { Incidents } from './pages/Incidents';
import { Alerts } from './pages/Alerts';
import { Quality } from './pages/Quality';
import { Settings } from './pages/Settings';

export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route element={<ProtectedRoute />}>
            <Route element={<Layout />}>
              <Route path="/"            element={<Overview />} />
              <Route path="/heatmap"     element={<Heatmap />} />
              <Route path="/predictions" element={<Predictions />} />
              <Route path="/incidents"   element={<Incidents />} />
              <Route path="/alerts"      element={<Alerts />} />
              <Route path="/quality"     element={<Quality />} />
              <Route path="/settings"    element={<Settings />} />
              <Route path="*"            element={<Navigate to="/" replace />} />
            </Route>
          </Route>
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  );
}