# real-time-sales-stock-pipeline
Real-Time Sales & Stock Pipeline
Apache Dask • DuckDB (Iceberg-like) • Snowflake • Tableau
Bu proje, bir e-ticaret şirketinin satış ve stok verilerini gerçek zamanlı analiz edebilmesi için uçtan uca bir veri hattı (pipeline) oluşturur. Veri üretiminden dashboard’a kadar tüm süreç otomatikleştirilmiştir.
📌 Proje Bileşenleri (Özet)
1. Veri Üretimi & Temizleme
Python ile satış ve stok verisi üretildi (generate_data.py)
Veri CSV ve Parquet formatında oluşturuldu.
Snowflake uyumluluğu için metin düzenleme yapıldı.
2. Büyük Veri İşleme (Dask)
Günlük talep analizi
Bölgesel satış trendleri
Null temizliği, tarih formatları
İşlenmiş veriler data/processed/ klasörüne kaydedildi.
Script: dask_processing.py
3. Iceberg Benzeri Lakehouse (DuckDB)
Parquet dosyaları region + date partition mantığıyla saklandı.
Satış & stok verileri için versiyon klasörleri oluşturuldu.
"current_stock" adlı analitik görünüm tanımlandı.
Script: iceberg_setup.py
4. Snowflake Veri Yükleme
COPY komutları ile veri Snowflake tablosuna aktarıldı.
SQL ile analizler yapıldı:
En çok satılan ürünler
Bölgesel satış eğilimleri
Stok uyarıları
Script: snowflake_load.py
5. Tableau Dashboard
Hazırlanan dashboard aşağıdaki grafiklerden oluşur:
Bölgesel satış trendi
Günlük talep analizi
En çok satılan ürünler
Stok yetersizliği uyarıları
Dashboard dosyaları /dashboards/ klasöründe bulunur.
📁 Klasör Yapısı
scripts/       → Pipeline scriptleri
data/raw/      → Ham veriler
data/processed/→ Dask çıktıları
warehouse/     → Iceberg-like Parquet Lakehouse
dashboards/    → Tableau dashboard
docs/          → Rapor ve ek belgeler
🧾 Sonuç
Bu proje; veri üretimi, büyük veri işleme, lakehouse tasarımı, bulut veri ambarı ve dashboard oluşturmayı kapsayan tam bir uçtan uca veri mühendisliği çözümüdür.
