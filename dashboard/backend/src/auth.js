import jwt from "jsonwebtoken";

const COOKIE_NAME = "energy_session";

function parseUsers() {
  const raw = process.env.AUTH_USERS || "admin:admin123:admin,operator:operator123:operator";
  return raw.split(",").map(entry => {
    const [username, password, role] = entry.split(":");
    return { username, password, role: role || "operator" };
  }).filter(u => u.username && u.password);
}

export function findUser(username, password) {
  return parseUsers().find(u => u.username === username && u.password === password) || null;
}

function getSecret() {
  const s = process.env.JWT_SECRET;
  if (!s || s.length < 16) {
    console.warn("[auth] JWT_SECRET is missing or too short — using insecure fallback. Fix .env before deploy.");
    return "dev-fallback-insecure-do-not-use-in-prod";
  }
  return s;
}

export function issueToken(user) {
  const payload = { sub: user.username, role: user.role };
  return jwt.sign(payload, getSecret(), { expiresIn: process.env.JWT_EXPIRES_IN || "8h" });
}

export function verifyToken(token) {
  try {
    return jwt.verify(token, getSecret());
  } catch {
    return null;
  }
}

export function setSessionCookie(res, token) {
  const maxAge = 8 * 60 * 60 * 1000;
  res.cookie(COOKIE_NAME, token, {
    httpOnly: true,
    sameSite: "lax",
    secure: false,
    maxAge,
    path: "/",
  });
}

export function clearSessionCookie(res) {
  res.clearCookie(COOKIE_NAME, { path: "/" });
}

export const SESSION_COOKIE = COOKIE_NAME;
