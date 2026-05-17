import { Router } from "express";
import { findUser, issueToken, setSessionCookie, clearSessionCookie, verifyToken, SESSION_COOKIE } from "../auth.js";

const router = Router();

router.post("/login", (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: "username and password required" });
  const user = findUser(username, password);
  if (!user) return res.status(401).json({ error: "invalid credentials" });
  const token = issueToken(user);
  setSessionCookie(res, token);
  res.json({ user: { username: user.username, role: user.role } });
});

router.post("/logout", (_req, res) => {
  clearSessionCookie(res);
  res.json({ ok: true });
});

router.get("/me", (req, res) => {
  const token = req.cookies?.[SESSION_COOKIE];
  if (!token) return res.status(401).json({ error: "not authenticated" });
  const payload = verifyToken(token);
  if (!payload) return res.status(401).json({ error: "invalid or expired session" });
  res.json({ user: { username: payload.sub, role: payload.role } });
});

export default router;
