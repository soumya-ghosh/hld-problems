# API Gateway vs Load Balancer vs Reverse Proxy

## TL;DR

| Aspect | Reverse Proxy | Load Balancer | API Gateway |
|---|---|---|---|
| **Core job** | Sits in front of servers, forwards client requests | Distributes traffic across multiple server instances | Single entry point for API consumers with cross-cutting concerns |
| **OSI Layer** | L7 (HTTP) | L4 (TCP/UDP) or L7 (HTTP) | L7 (HTTP/REST/gRPC) |
| **Knows about your API semantics?** | No | No | Yes |
| **Examples** | Nginx, HAProxy, Envoy | AWS ALB/NLB, Nginx, HAProxy | Kong, AWS API Gateway, Apigee, Zuul |

They overlap significantly. A reverse proxy *can* load balance. A load balancer *can* act as a reverse proxy. An API gateway *is* a reverse proxy + load balancer + a lot more. The difference is **intent and feature scope**.

---

## Reverse Proxy

A server that sits between clients and backend servers. Clients talk to the proxy, the proxy talks to the backend. The client never knows the backend exists.

```
Client ──▶ Reverse Proxy ──▶ Backend Server(s)
```

### What it does

- **Hides backend topology** — clients see one IP, not 10 servers
- **SSL termination** — decrypt HTTPS at the proxy, talk plain HTTP to backends
- **Caching** — serve static content or cached responses without hitting backends
- **Compression** — gzip responses before sending to client
- **Request/response rewriting** — modify headers, paths, etc.
- **Security** — shields backends from direct internet exposure, basic DDoS protection

### What it does NOT do

- No awareness of API contracts, versioning, or consumers
- No built-in auth, rate limiting, or API key management
- No traffic splitting by API route semantics (though you can route by path)

### When to use

You have a few backend servers and need SSL termination, caching, or to hide your infra. Classic Nginx/HAProxy setup.

---

## Load Balancer

Distributes incoming traffic across multiple instances of the **same** service to improve availability and throughput.

```
                        ┌──▶ Server 1
Client ──▶ Load Balancer├──▶ Server 2
                        └──▶ Server 3
```

### L4 vs L7

| | L4 (Transport) | L7 (Application) |
|---|---|---|
| **Operates on** | TCP/UDP packets | HTTP requests |
| **Routing decision** | IP + port | URL path, headers, cookies, body |
| **Speed** | Faster (no payload inspection) | Slower (parses HTTP) |
| **Use case** | Raw throughput, non-HTTP protocols | Content-based routing, sticky sessions |
| **Example** | AWS NLB, HAProxy TCP mode | AWS ALB, Nginx upstream |

### Algorithms

- **Round Robin** — rotate through servers sequentially
- **Least Connections** — send to the server with fewest active connections
- **Weighted** — servers get traffic proportional to capacity
- **IP Hash** — same client IP always hits same server (poor man's sticky sessions)
- **Consistent Hashing** — distribute by key (useful for caches)
> **Q:** Why poor man's sticky sessions? What are some other approaches to sticky sessions?
>
>> **A:** IP Hash breaks when clients share IPs (NAT, corporate proxies, CGNAT) — multiple users pin to the same server, and a roaming user loses affinity.
>>
>> Better approaches:
>> - **Cookie-based** — LB injects a cookie (`SERVERID=backend2`) on first response; routes by cookie thereafter. Most common for HTTP (ALB, Nginx, HAProxy support it).
>> - **Session ID routing** — app-level token in header/URL that the LB extracts for routing. Works for APIs and non-browser clients.
>> - **Centralized session store** — Redis/Memcached holds state; any backend can serve any request. Eliminates stickiness entirely. Preferred at scale.
>> - **Stateless tokens (JWT)** — session data in the token itself. No server-side state, no affinity needed.

### Health Checks

LBs actively probe backends (TCP connect, HTTP GET /health) and remove unhealthy ones from the pool. This is a critical differentiator from a dumb reverse proxy.

### What it does NOT do

- No API-level concerns (auth, rate limiting, transformation)
- No API versioning or consumer management
- Doesn't understand your business logic routes

---

## API Gateway

A **superset** of reverse proxy + load balancer, purpose-built for managing API traffic.

```
                                    ┌──▶ User Service
Client ──▶ API Gateway (auth,      ├──▶ Order Service
            rate limit, transform)  ├──▶ Payment Service
                                    └──▶ Notification Service
```

### What it adds on top

| Feature | Description |
|---|---|
| **Authentication & Authorization** | Validate JWT, OAuth2, API keys before requests hit backends |
| **Rate Limiting & Throttling** | Per-consumer, per-route, sliding window, token bucket |
| **Request/Response Transformation** | Reshape payloads, add/remove headers, protocol translation (REST → gRPC) |
| **API Versioning** | Route `/v1/users` → service A, `/v2/users` → service B |
| **API Composition / Aggregation** | Fan out one client request to multiple microservices, merge responses |
| **Circuit Breaking** | Stop forwarding to a failing service, return fallback |
| **Canary / Blue-Green Deployments** | Route 5% traffic to new version, 95% to stable |
| **Analytics & Monitoring** | Track latency, error rates, usage per consumer/route |
| **Developer Portal** | API docs, key management, onboarding |
| **Request Validation** | Validate against OpenAPI schema before forwarding |

### The cost

- **Single point of failure** — needs to be HA itself (clustered, multi-AZ)
- **Latency overhead** — every request goes through it, adds ~1-5ms
- **Complexity** — more config, more moving parts
- **Can become a monolith** — if you stuff too much logic in it (avoid business logic in the gateway)

---

## How They Relate

```
┌─────────────────────────────────────────────────┐
│                  API Gateway                     │
│  ┌───────────────────────────────────────────┐  │
│  │            Reverse Proxy                   │  │
│  │  ┌─────────────────────────────────────┐  │  │
│  │  │         Load Balancer                │  │  │
│  │  └─────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────┘  │
│  + Auth, Rate Limiting, Transforms, Analytics   │
└─────────────────────────────────────────────────┘
```

An API Gateway **contains** a reverse proxy which **contains** load balancing. Each layer adds more intelligence.

---

## When to Use What

| Scenario | Use |
|---|---|
| Single app, need SSL + caching | **Reverse Proxy** (Nginx) |
| Single service, multiple replicas | **Load Balancer** (ALB/NLB) |
| Microservices with multiple consumers | **API Gateway** (Kong, AWS API GW) |
| Internal service-to-service traffic | **Service Mesh sidecar** (Envoy via Istio) — not a gateway |
| High-throughput non-HTTP (TCP/UDP) | **L4 Load Balancer** (NLB, HAProxy) |

---

## Real-World Architecture

In practice, you'll often see **all three** in the same system:

```
Internet
   │
   ▼
┌──────────┐
│ CDN/WAF  │  ← Cloudflare, AWS CloudFront
└────┬─────┘
     ▼
┌──────────┐
│   NLB    │  ← L4 Load Balancer (distributes across API GW instances)
└────┬─────┘
     ▼
┌──────────────┐
│ API Gateway  │  ← Auth, rate limit, routing (Kong / AWS API GW)
│  (cluster)   │
└────┬─────┘
     ▼
┌──────────┐
│   ALB    │  ← L7 Load Balancer (per-service, optional if using k8s Service)
└────┬─────┘
     ▼
┌──────────────────┐
│  Microservices   │
│  (ECS/K8s pods)  │
└──────────────────┘
```

The NLB load-balances across API Gateway instances. The API Gateway handles cross-cutting concerns. The ALB (or k8s Service) load-balances across pods of each microservice.

---

## Common Confusion

**"Isn't Nginx all three?"** — Sort of. Nginx can reverse proxy, load balance, and with OpenResty/plugins do some gateway-like things. But it doesn't natively handle JWT validation, rate limiting per API key, request schema validation, or developer portals. That's why purpose-built API gateways exist.

**"Do I need an API Gateway if I have a service mesh?"** — Yes, for **north-south** traffic (external clients → your services). The service mesh handles **east-west** traffic (service → service). They're complementary, not competing.

**"Can my API Gateway be the load balancer too?"** — Yes, most API gateways have built-in load balancing. But you still want an LB *in front of* the gateway itself for HA.
