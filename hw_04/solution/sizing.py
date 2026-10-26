#!/usr/bin/env python3
"""Воспроизводимая модель сайзинга для ДЗ 4 (сервис бронирования).

Запуск:
    python3 sizing.py

Все допущения собраны в блоке ASSUMPTIONS и в таблицах ниже, чтобы расчёт
можно было проверить и пересчитать при изменении вводных. Цены — публичные
on-demand листы AWS eu-central-1, округлены, зафиксированы на 2026-10.
"""

from math import ceil

DAYS_IN_MONTH = 30
SECONDS_IN_DAY = 86_400

# ---------------------------------------------------------------------------
# 1. Вводные и допущения
# ---------------------------------------------------------------------------

ASSUMPTIONS = {
    "MAU": 3_000_000,
    "DAU": 300_000,
    "bookings_per_user_month": 1,
    "peak_factor": 5.0,          # сезонный пик (час пика / среднегодовой час)
    "growth_3y": 1.8,           # рост трафика за 3 года
    "storage_growth_3y": 1.7,   # итог по storage_table(); по сущностям — 1.2–3.9
}

BOOKINGS_DAY = ASSUMPTIONS["MAU"] * ASSUMPTIONS["bookings_per_user_month"] / DAYS_IN_MONTH
PEAK = ASSUMPTIONS["peak_factor"]

# name, service, kind, ops/day, request KB, response KB, cpu ms, p99
ENDPOINTS = [
    ("GET /search",              "Search",   "read",  3_600_000,  8, 20, 25, "800 ms"),
    ("GET /offers/{id}",         "Search",   "read",  7_200_000,  2, 10, 25, "300 ms"),
    ("GET /properties/{id}",     "Catalog",  "read",    900_000,  1,  8,  5, "150 ms"),
    ("GET /bookings",            "Booking",  "read",    600_000,  1, 12,  5, "200 ms"),
    ("GET /bookings/{id}/status","Booking",  "read",    110_000,  1,  1,  1, "SSE"),
    ("POST /pricing/quote",      "Inventory","read",    300_000,  2,  2, 10, "250 ms"),
    ("POST /holds",              "Inventory","write",   120_000,  2,  3, 10, "250 ms"),
    ("POST /bookings",           "Booking",  "write",   105_000,  2,  3, 20, "300 ms"),
    ("POST /bookings/{id}/pay",  "Booking",  "write",   110_000,  2,  3, 20, "800 ms"),
    ("POST /bookings/{id}/cancel","Booking", "write",    12_000,  2,  3, 15, "300 ms"),
    ("POST /payments/callback",  "Payment",  "write",   110_000,  2,  1,  5, "200 ms"),
    ("POST /supplier/webhook",   "Supplier", "write",20_000_000, 20,  1,  2, "500 ms"),
]

# Внутренние синхронные вызовы (не видны клиенту, но создают нагрузку).
INTERNAL_CALLS = [
    ("BFF → Search (search+offer)", 625.0, 5),
    ("Search → Catalog (батч)",     208.3, 20),
    ("Search → Inventory (снимок)", 208.3, 20),
    ("BFF → Catalog",                52.1,  8),
    ("BFF → Inventory",              24.3,  3),
    ("BFF → Booking",                49.9,  3),
    ("Booking → Payment",             6.4,  2),
    ("Booking → Inventory",           6.4,  2),
    ("Booking → Supplier",            6.4,  2),
]

# ---------------------------------------------------------------------------
# 2. RPS
# ---------------------------------------------------------------------------

def rps(ops_day: float) -> tuple[float, float]:
    avg = ops_day / SECONDS_IN_DAY
    return avg, avg * PEAK


def endpoint_rps_table() -> list[str]:
    rows = [
        "| Эндпоинт | Сервис | Тип | Операций/день | Ср. RPS | Пик RPS (×5) | p99 |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for name, service, kind, ops, _, _, _, p99 in ENDPOINTS:
        avg, peak = rps(ops)
        rows.append(
            f"| `{name}` | {service} | {kind} | {ops:,} | {avg:.1f} | {peak:.0f} | {p99} |"
        )
    total_ops = sum(e[3] for e in ENDPOINTS if e[1] != "Supplier")
    avg, peak = rps(total_ops)
    rows.append(
        f"| **Итого внешний API без supplier** | — | — | **{total_ops:,}** | **{avg:.1f}** | **{peak:.0f}** | — |"
    )
    return rows


def service_load() -> list[dict]:
    """Пиковая нагрузка по сервисам с учётом внутренних вызовов.

    Значения получены из ENDPOINTS/INTERNAL_CALLS с округлением и поправкой на
    батчинг/кэш (часть вызовов не доходит до сервиса); для наглядности заданы
    явно, чтобы таблица ресурсов читалась без пересчёта вручную.
    """
    return [
        # service, peak RPS, cpu_ms, pod_vcpu, ram_gb, min_pods
        {"service": "BFF / API Gateway", "rps": 753.9, "cpu": 3,  "vcpu": 2, "ram": 3.0, "min": 6},
        {"service": "Search & Offer",    "rps": 625.0, "cpu": 25, "vcpu": 2, "ram": 6.0, "min": 6},
        {"service": "Catalog",           "rps": 302.0, "cpu": 5,  "vcpu": 1, "ram": 2.0, "min": 6},
        {"service": "Inventory & Pricing","rps": 245.1,"cpu": 10, "vcpu": 1, "ram": 4.0, "min": 6},
        {"service": "Booking",           "rps": 49.9,  "cpu": 20, "vcpu": 1, "ram": 1.5, "min": 6},
        {"service": "Payment",           "rps": 13.7,  "cpu": 20, "vcpu": 1, "ram": 1.5, "min": 6},
        {"service": "Supplier Integration","rps": 1169.8,"cpu": 2, "vcpu": 1, "ram": 1.5, "min": 6},
        {"service": "Notification",      "rps": 18.0,  "cpu": 5,  "vcpu": 1, "ram": 0.5, "min": 6},
    ]


def resource_table() -> tuple[list[str], int, int, int]:
    target = 0.60  # целевая утилизация CPU на пике
    rows = [
        "| Сервис | Пик RPS | CPU/запрос, мс | RPS на vCPU (60%) | Нужно vCPU | Поды | vCPU итого | RAM, ГБ |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    total_vcpu = total_ram = total_pods = 0
    for s in service_load():
        rps_per_vcpu = (1000 / s["cpu"]) * target
        need_vcpu = s["rps"] / rps_per_vcpu
        pods = max(s["min"], ceil(need_vcpu / s["vcpu"]))
        if s["service"] == "Search & Offer":
            pods = 14  # capacity + 3 AZ, кратно 2 подам на AZ
        vcpu = pods * s["vcpu"]
        ram = pods * s["ram"]
        total_vcpu += vcpu
        total_ram += ram
        total_pods += pods
        rows.append(
            f"| {s['service']} | {s['rps']:.0f} | {s['cpu']} | {rps_per_vcpu:.0f} | "
            f"{need_vcpu:.1f} | {pods} | {vcpu} | {ram:.0f} |"
        )
    rows.append(
        f"| **Итого** | — | — | — | — | **{total_pods}** | "
        f"**{total_vcpu}** | **{total_ram:.0f}** |"
    )
    nodes = ceil(total_vcpu / 8 / 0.7)  # ≥30% свободной CPU-ёмкости на нодах
    return rows, total_vcpu, total_ram, nodes


# ---------------------------------------------------------------------------
# 3. Хранилище
# ---------------------------------------------------------------------------

GB = 1024 ** 3          # байт в GiB (для единообразия считаем KB=1024 B)
TB = 1024 ** 4


def _tb(gb: float) -> str:
    """GB → TB для таблицы."""
    return f"{gb / 1024:.2f} TB"


def storage_table() -> list[str]:
    rows = [
        "| Сущность | Расчёт | 1 год | 3 года | Комментарий |",
        "|---|---|---:|---:|---|",
    ]
    # Catalog MongoDB: 1M properties * 30 KB, overhead 1.3, RF3
    cat_1y = 1_000_000 * 30 / 1024 / 1024 * 1.3 * 3  # GB
    cat_3y = cat_1y * 1.5
    rows.append(
        f"| Catalog (MongoDB) | 1M × 30 KB × 1.3 × RF3 | {_tb(cat_1y)} | {_tb(cat_3y)} | версии карточек за 3 года |"
    )
    # Search index: 1M * 20KB, overhead 1.3, 1 replica
    os_1y = 1_000_000 * 20 / 1024 / 1024 * 1.3 * 2
    rows.append(
        f"| Search (OpenSearch) | 1M × 20 KB × 1.3 × 2 копии | {_tb(os_1y)} | {_tb(os_1y*1.2)} | rolling-индекс + snapshots |"
    )
    # Scylla: 600K properties * 3 rates * 365 days = 657M cells * 64B * 1.3 * RF3
    cells = 600_000 * 3 * 365
    scy_1y = cells * 64 / GB * 1.3 * 3  # GB
    rows.append(
        f"| Inventory (ScyllaDB) | 657M ячеек × 64 B × 1.3 × RF3 | {_tb(scy_1y)} | {_tb(scy_1y*1.2)} | календарь rolling 365 дней |"
    )
    # Booking PG: 36M/year * 3KB * 1.8 * 1.5 backup, 3 years, +30% growth
    book_1y = 36_000_000 * 3 / 1024 / 1024 * 1.8 * 1.5
    rows.append(
        f"| Booking (PostgreSQL) | 36M × 3 KB × 1.8 × 1.5 | {_tb(book_1y)} | {_tb(book_1y*3*1.3)} | заказы, гости, Saga, outbox |"
    )
    # Payment PG: 40M/year * 1.5KB * 2 * 1.5, 3 years, +30%
    pay_1y = 40_000_000 * 1.5 / 1024 / 1024 * 2 * 1.5
    rows.append(
        f"| Payment (PostgreSQL) | 40M × 1.5 KB × 2 × 1.5 | {_tb(pay_1y)} | {_tb(pay_1y*3*1.3)} | платежи, возвраты, идемпотентность |"
    )
    # Supplier PG hot 7 days
    sup_1y = 20_000_000 * 7 * 1.5 / 1024 / 1024 * 2
    rows.append(
        f"| Supplier (PostgreSQL) | 20M/день × 7 дней × 1.5 KB × 2 | {_tb(sup_1y)} | 2.00 TB | hot 7 дней + холодный архив |"
    )
    # Kafka 7 days RF3
    kafka_1y = (20_000_000 + 1_000_000) * 7 * 1.5 / 1024 / 1024 * 3 * 1.3
    rows.append(
        f"| Kafka | 21M/день × 7 дней × 1.5 KB × RF3 × 1.3 | {_tb(kafka_1y)} | {_tb(kafka_1y*1.2)} | tiered storage — резерв |"
    )
    # Object storage
    rows.append("| S3: фото и превью | 1M × 20 фото × 300 KB + превью | 8.00 TB | 12.00 TB | origin для CDN |")
    rows.append("| S3: бэкапы, логи, архив | PG + OpenSearch + webhook-архив | 3.00 TB | 5.00 TB | lifecycle в IA/Glacier |")
    total_1y = cat_1y + os_1y + scy_1y + book_1y + pay_1y + sup_1y + kafka_1y + 8 * 1024 + 3 * 1024
    total_3y = (
        cat_3y + os_1y * 1.2 + scy_1y * 1.2 + book_1y * 3 * 1.3 + pay_1y * 3 * 1.3
        + 2 * 1024 + kafka_1y * 1.2 + 12 * 1024 + 5 * 1024
    )
    rows.append(
        f"| **Итого** | — | **{_tb(total_1y)}** | **{_tb(total_3y)}** | без учёта реплик CDN |"
    )
    return rows, total_1y, total_3y


# ---------------------------------------------------------------------------
# 4. Bandwidth
# ---------------------------------------------------------------------------

def bandwidth() -> dict:
    api_egress_tb = 0.0
    api_ingress_tb = 0.0
    api_egress_mbps = 0.0
    api_ingress_mbps = 0.0
    for name, service, kind, ops, req_kb, resp_kb, _, _ in ENDPOINTS:
        if service == "Supplier":
            continue
        monthly = ops * DAYS_IN_MONTH
        api_egress_tb += monthly * resp_kb / 1024 ** 3
        api_ingress_tb += monthly * req_kb / 1024 ** 3
        _, peak = rps(ops)
        api_egress_mbps += peak * resp_kb * 8 / 1024
        api_ingress_mbps += peak * req_kb * 8 / 1024

    # Supplier webhooks отдельно: входящий трафик.
    _, sup_peak = rps(20_000_000)
    sup_ingress_mbps = sup_peak * 20 * 8 / 1024
    sup_ingress_tb = 20_000_000 * DAYS_IN_MONTH * 20 / 1024 ** 3

    cdn_tb = 18.0
    cdn_peak_mbps = 278.0
    total_egress_tb = api_egress_tb + cdn_tb
    total_peak_egress = api_egress_mbps + cdn_peak_mbps
    return {
        "api_egress_tb": api_egress_tb,
        "api_ingress_tb": api_ingress_tb,
        "api_egress_mbps": api_egress_mbps,
        "api_ingress_mbps": api_ingress_mbps,
        "sup_ingress_mbps": sup_ingress_mbps,
        "sup_ingress_tb": sup_ingress_tb,
        "cdn_tb": cdn_tb,
        "cdn_peak_mbps": cdn_peak_mbps,
        "total_egress_tb": total_egress_tb,
        "total_peak_egress": total_peak_egress,
    }


# ---------------------------------------------------------------------------
# 5. Стоимость (AWS eu-central-1, on-demand, USD/мес)
# ---------------------------------------------------------------------------

COST_ITEMS = [
    # category, item, cost
    ("Compute", "EKS workers: 14 × c6i.2xlarge (8 vCPU/16 GB)", 14 * 0.374 * 730),
    ("Compute", "EKS control plane", 73),
    ("Compute", "EBS для нод: 700 GB gp3", 700 * 0.088),
    ("Databases", "OpenSearch: 3 × r6g.2xlarge.search + 3 × m6g.large.search + 600 GB gp3", 1112),
    ("Databases", "ScyllaDB: 6 × i4i.xlarge (4 vCPU/32 GB/937 GB NVMe)", 6 * 0.497 * 730),
    ("Databases", "Booking PostgreSQL: r6g.xlarge Multi-AZ + read replica + 1 TB", 1505),
    ("Databases", "Payment PostgreSQL: r6g.large Multi-AZ + read replica + 500 GB", 753),
    ("Databases", "Supplier PostgreSQL: r6g.large Multi-AZ + 500 GB", 560),
    ("Databases", "ElastiCache Redis: 3 шарда × 1 реплика, cache.r6g.large", 6 * 0.249 * 730),
    ("Databases", "MSK: 3 × kafka.m5.xlarge + 3 TB EBS + transfer", 1442),
    ("Storage/CDN", "S3: 15 TB с версиями (фото, превью, бэкапы, логи)", 418),
    ("Storage/CDN", "CloudFront: 22.8 TB egress + requests (18 TB CDN + 4.5 TB API)", 1970),
    ("Network", "NAT Gateway + cross-AZ traffic", 264),
    ("Network", "ALB/NLB", 170),
    ("Observability", "CloudWatch logs/metrics + X-Ray", 750),
    ("Security", "WAF, KMS, Secrets Manager", 200),
    ("DR", "Cross-region backups, snapshots", 300),
]


def cost_table() -> tuple[list[str], float, float, float]:
    rows = [
        "| Категория | Компонент | $/мес |",
        "|---|---|---:|",
    ]
    subtotal = 0.0
    for category, item, cost in COST_ITEMS:
        subtotal += cost
        rows.append(f"| {category} | {item} | {cost:,.0f} |")
    support = subtotal * 0.07
    reserve = (subtotal + support) * 0.10
    total = subtotal + support + reserve
    rows.append(f"| | **Инфраструктура** | **{subtotal:,.0f}** |")
    rows.append(f"| | AWS Business Support (7%) | {support:,.0f} |")
    rows.append(f"| | Резерв на непредвиденное (10%) | {reserve:,.0f} |")
    rows.append(f"| | **Итого в месяц** | **{total:,.0f}** |")
    rows.append(f"| | **Итого в год** | **{total*12:,.0f}** |")
    return rows, subtotal, total, support + reserve


def optimization_table() -> tuple[list[str], float]:
    rows = [
        "| # | Мероприятие | Экономия, $/мес | Риск и как снимаем |",
        "|---|---|---:|---|",
    ]
    items = [
        ("1", "1-летние Savings Plans / RI на 60% steady-state (compute, RDS, OpenSearch, Redis, MSK)", 1533, "commitment; покрываем только 60% базы, 1-year no upfront"),
        ("2", "Сезонный autoscaling + 20–25% Spot для Supplier/Notification", 1112, "spot interruption; PDB, mixed instances, on-demand база 75%"),
        ("3", "CDN: AVIF/WebP, responsive-превью, immutable cache; lifecycle S3", 800, "качество картинок; fallback JPEG, A/B"),
        ("4", "ScyllaDB 6→4 узла + Kafka tiered storage (24 ч hot, 7 дней cold)", 998, "пиковая запись; load-test ×2, RF3 сохраняем"),
        ("5", "OpenSearch right-size + кэш поиска 45 с + singleflight", 384, "p99 поиска; canary, 3 data nodes, SLO-алерт"),
    ]
    total = 0
    for num, what, saving, risk in items:
        total += saving
        rows.append(f"| {num} | {what} | {saving:,} | {risk} |")
    rows.append(f"| | **Итого прямая экономия** | **{total:,}** | — |")
    return rows, total


# ---------------------------------------------------------------------------
# 6. Вывод
# ---------------------------------------------------------------------------

def main() -> None:
    print("# Автоматический расчёт hw_04\n")
    a = ASSUMPTIONS
    print(
        f"Допущения: MAU {a['MAU']:,}, DAU {a['DAU']:,}, "
        f"пик ×{a['peak_factor']:g}, рост трафика ×{a['growth_3y']:g}, "
        f"рост данных ×{a['storage_growth_3y']:g}.\n"
    )
    print("## RPS\n")
    print("\n".join(endpoint_rps_table()))
    sse_avg = 110_000 / SECONDS_IN_DAY * 45
    print(f"\nSSE: {sse_avg:.0f} соединений в среднем, {sse_avg * PEAK:.0f} в пике (закон Литтла).")
    print(f"Через 3 года внешний API: {762 * a['growth_3y']:.0f} RPS.")
    print("\n## Ресурсы\n")
    rows, vcpu, ram, nodes = resource_table()
    print("\n".join(rows))
    print(f"\nВсего vCPU: {vcpu}, RAM: {ram:.0f} GB, нод c6i.2xlarge: {nodes}.")
    print("\n## Хранилище\n")
    rows, total_1y, total_3y = storage_table()
    print("\n".join(rows))
    print(f"Рост хранилища: ×{total_3y / total_1y:.1f} (в ASSUMPTIONS: ×{a['storage_growth_3y']:g}; по сущностям — 1.2–3.9).")
    print("\n## Bandwidth\n")
    b = bandwidth()
    print(f"API egress: {b['api_egress_tb']:.2f} TB/мес, {b['api_egress_mbps']:.0f} Mbps пик")
    print(f"API ingress: {b['api_ingress_tb']:.2f} TB/мес, {b['api_ingress_mbps']:.0f} Mbps пик")
    print(f"Supplier ingress: {b['sup_ingress_tb']:.2f} TB/мес, {b['sup_ingress_mbps']:.0f} Mbps пик")
    print(f"CDN: {b['cdn_tb']:.2f} TB/мес, {b['cdn_peak_mbps']:.0f} Mbps пик")
    print(f"Итого egress: {b['total_egress_tb']:.2f} TB/мес, {b['total_peak_egress']:.0f} Mbps пик")
    print("\n## Стоимость\n")
    rows, subtotal, total, extra = cost_table()
    print("\n".join(rows))
    print(f"\nИнфраструктура: ${subtotal:,.0f}; всего: ${total:,.0f}; support+резерв: ${extra:,.0f}")
    print("Сумма строк может отличаться от итога на $1 из-за округления статей.")
    print("\n## Оптимизация\n")
    rows, saving = optimization_table()
    print("\n".join(rows))
    print(f"\nПрямая экономия: ${saving:,.0f}/мес")


if __name__ == "__main__":
    main()
