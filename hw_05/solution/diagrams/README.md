# Схемы ДЗ 5

Исходники в Mermaid; те же схемы встроены в [../README.md](../README.md) и
рендерятся на GitHub.

| Файл | Что показывает |
|---|---|
| [1_replication.mmd](1_replication.mmd) | Репликация БД: Multi-AZ в регионе A, sync для Booking/Payment, async для остальных, warm standby в регионе B |
| [2_dr_regions.mmd](2_dr_regions.mmd) | DR при падении региона: детект, fencing, promotion, переключение DNS и reconciliation |

Легенда: `==>` — синхронная репликация; `-.->` — асинхронная репликация или
управляющее действие. RTO/RPO и порядок promotion — в §1.2–1.5 решения.
