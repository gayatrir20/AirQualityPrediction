• Extracts time-based features from datetime index to capture cyclical temporal patterns observed in EDA like hour, month, weekend, day of week.
• Encodes short-term historical patterns like lagging by 1, 3, 6, 12, and 24 hours and captures immediate precedents to current readings.
• Creates rolling window statistics to smooth short-term fluctuations and identify emerging trends.
• Handles missing values and removes irrelevant columns.
• Temporal Validation Split as 80% training data size and 20% testing data size.
• In total, 23 features, including raw measurements, temporal features, lagged features, and rolling features.
