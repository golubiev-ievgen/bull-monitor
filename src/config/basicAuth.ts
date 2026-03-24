import { Request, Response, NextFunction } from "express";

const EXPECTED_USER = "admin";

export function basicAuth(req: Request, res: Response, next: NextFunction) {
  const expectedPassword = process.env.QUEUE_ADMIN_PASS;

  if (!expectedPassword) {
    console.error("QUEUE_ADMIN_PASS env var is not set");
    return res.status(500).send("Server misconfigured");
  }

  const authHeader = req.headers["authorization"];

  if (!authHeader || !authHeader.startsWith("Basic ")) {
    res.setHeader("WWW-Authenticate", 'Basic realm="Restricted"');
    return res.status(401).send("Authentication required");
  }

  const base64Credentials = authHeader.slice("Basic ".length).trim();

  let username = "";
  let password = "";

  try {
    const credentials = Buffer.from(base64Credentials, "base64").toString("utf8");
    const [user, pass] = credentials.split(":");
    username = user;
    password = pass;
  } catch (e) {
    res.setHeader("WWW-Authenticate", 'Basic realm="Restricted"');
    return res.status(401).send("Invalid auth header");
  }

  if (username !== EXPECTED_USER || password !== expectedPassword) {
    res.setHeader("WWW-Authenticate", 'Basic realm="Restricted"');
    return res.status(401).send("Invalid credentials");
  }

  (req as any).user = { username };

  return next();
}