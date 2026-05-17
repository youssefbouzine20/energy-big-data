import { SESSION_COOKIE, verifyToken } from "./auth.js";

export function requireAuth(req, res, next) {
  const token = req.cookies?.[SESSION_COOKIE];
  if (!token) return res.status(401).json({ error: "not authenticated" });
  const payload = verifyToken(token);
  if (!payload) return res.status(401).json({ error: "invalid or expired session" });
  req.user = { username: payload.sub, role: payload.role };
  next();
}

export function requireRole(...roles) {
  return (req, res, next) => {
    if (!req.user) return res.status(401).json({ error: "not authenticated" });
    if (!roles.includes(req.user.role)) return res.status(403).json({ error: "forbidden" });
    next();
  };
}
