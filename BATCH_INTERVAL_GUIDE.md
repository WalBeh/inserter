# Batch Interval Guide

A comprehensive guide to understanding and optimizing the `--batch-interval` parameter for maximum database performance.

## 🕐 What is Batch Interval?

The `--batch-interval` parameter controls the **pause time in seconds** between each batch insertion. It's one of the most important parameters for controlling load pressure on your CrateDB instance.

### Basic Workflow
```
1. Generate batch of N records
2. Insert batch into database
3. Wait for --batch-interval seconds  ⏱️
4. Repeat until duration expires
```

## 📊 Performance Impact Matrix

| Batch Interval | Load Level | Use Case | Records/sec (est.) |
|----------------|------------|----------|-------------------|
| `2.0` | Very Light | Development/Testing | ~50-500 |
| `1.0` | Light | Gentle Load Testing | ~100-1,000 |
| `0.5` | Moderate | Standard Testing | ~200-2,000 |
| `0.1` | High | Performance Testing | ~1,000-10,000 |
| `0.05` | Very High | Stress Testing | ~2,000-20,000 |
| `0.01` | Extreme | Maximum Pressure | ~5,000-50,000 |
| `0` | Maximum | Database Limits | ~10,000-100,000+ |

*Actual performance depends on batch size, threads, and database capacity*

## 🎯 Choosing the Right Interval

### Development & Testing
```bash
# Gentle load for development
./run.sh --table-name dev_test --duration 5 --batch-interval 1.0

# Watch data flow in real-time
./run.sh --table-name observe --duration 10 --batch-size 10 --batch-interval 2.0
```

### Performance Testing
```bash
# Baseline performance measurement
./run.sh --table-name baseline --duration 5 --batch-interval 0.1

# High throughput testing
./run.sh --table-name high_perf --duration 3 --batch-interval 0.01
```

### Stress Testing
```bash
# Maximum sustainable load
./run.sh --table-name stress --duration 2 --batch-interval 0 --threads 4

# Find the breaking point
./run.sh --table-name breaking_point --duration 1 --batch-interval 0 --threads 8 --batch-size 1000
```

## 🧮 Calculation Examples

### Single Thread Scenarios

**Conservative (interval=1.0, batch=100)**
- 100 records every 1 second
- **~100 records/second**
- Good for: Development, gentle testing

**Moderate (interval=0.1, batch=100)**
- 100 records every 0.1 seconds  
- **~1,000 records/second**
- Good for: Performance testing

**Aggressive (interval=0, batch=500)**
- 500 records as fast as possible
- **~5,000-50,000 records/second** (depends on DB)
- Good for: Finding limits

### Multi-Thread Scenarios

**4 Threads, interval=0.1, batch=200**
- Each thread: ~2,000 records/second
- **Total: ~8,000 records/second**

**8 Threads, interval=0, batch=1000**
- Each thread: Limited by database capacity
- **Total: Database maximum throughput**

## ⚡ Real-World Performance Examples

### Example 1: E-commerce Load Simulation
```bash
# Simulate realistic e-commerce traffic
./run.sh --table-name ecommerce_sim \
  --duration 10 \
  --threads 2 \
  --batch-size 50 \
  --batch-interval 0.5

# Result: ~200 records/second (realistic web traffic)
```

### Example 2: Data Migration Simulation
```bash
# Simulate high-speed data migration
./run.sh --table-name migration_test \
  --duration 5 \
  --threads 8 \
  --batch-size 1000 \
  --batch-interval 0

# Result: Maximum database throughput
```

### Example 3: IoT Data Ingestion
```bash
# Simulate continuous IoT data streams
./run.sh --table-name iot_stream \
  --duration 15 \
  --threads 4 \
  --batch-size 100 \
  --batch-interval 0.1

# Result: ~4,000 records/second (steady IoT load)
```

## 🔧 Optimization Strategies

### Finding Your Database Limit

1. **Start Conservative**
   ```bash
   ./run.sh --table-name test1 --duration 2 --batch-interval 0.1 --threads 1
   ```

2. **Increase Pressure Gradually**
   ```bash
   ./run.sh --table-name test2 --duration 2 --batch-interval 0.05 --threads 2
   ./run.sh --table-name test3 --duration 2 --batch-interval 0.01 --threads 4
   ./run.sh --table-name test4 --duration 2 --batch-interval 0 --threads 8
   ```

3. **Monitor for Errors**
   - Watch for connection timeouts
   - Check for memory issues
   - Look for disk I/O bottlenecks

### Tuning for Different Goals

**Maximum Sustained Throughput**
```bash
# Find the sweet spot without errors
./run.sh --table-name sustained \
  --duration 10 \
  --threads 4 \
  --batch-size 500 \
  --batch-interval 0.01
```

**Burst Load Testing**
```bash
# Short bursts of maximum load
./run.sh --table-name burst \
  --duration 1 \
  --threads 16 \
  --batch-size 2000 \
  --batch-interval 0
```

**Realistic Workload Simulation**
```bash
# Match your production patterns
./run.sh --table-name realistic \
  --duration 30 \
  --threads 2 \
  --batch-size 100 \
  --batch-interval 0.5
```

## 📈 Performance Monitoring Tips

### Key Metrics to Watch

1. **Current Rate**: Short-term records/second
2. **Average Rate**: Overall test performance  
3. **Error Count**: Failed insertions
4. **Database CPU**: Resource utilization
5. **Memory Usage**: Both client and database

### Warning Signs

- **Increasing Errors**: Reduce threads or increase interval
- **Declining Performance**: Database is saturated
- **High Memory Usage**: Reduce batch size
- **Connection Timeouts**: Increase interval slightly

## 🎛️ Advanced Techniques

### Variable Load Patterns

Create realistic load patterns by varying intervals:

```bash
# Morning peak (high load)
./run.sh --table-name morning_peak --duration 5 --batch-interval 0.01 --threads 6

# Afternoon lull (moderate load)  
./run.sh --table-name afternoon --duration 10 --batch-interval 0.5 --threads 2

# Evening surge (maximum load)
./run.sh --table-name evening_surge --duration 3 --batch-interval 0 --threads 12
```

### Progressive Load Testing

```bash
#!/bin/bash
# Progressive stress test
for interval in 1.0 0.5 0.1 0.05 0.01 0; do
  echo "Testing interval: $interval"
  ./run.sh --table-name "test_${interval}" \
    --duration 2 \
    --batch-interval $interval \
    --threads 4
done
```

## 🚨 Troubleshooting Common Issues

### Problem: Low Performance Despite interval=0
**Causes:**
- Database is the bottleneck
- Network latency
- Insufficient batch size
- Resource constraints

**Solutions:**
```bash
# Increase batch size
--batch-size 2000

# Add more threads
--threads 8

# Check database resources
```

### Problem: High Error Rates
**Causes:**
- Too aggressive settings
- Database overload
- Connection limits

**Solutions:**
```bash
# Reduce pressure
--batch-interval 0.1

# Fewer threads
--threads 2

# Smaller batches
--batch-size 100
```

### Problem: Inconsistent Performance
**Causes:**
- Database cache effects
- Background processes
- Network fluctuations

**Solutions:**
- Run longer tests (10+ minutes)
- Monitor system resources
- Use consistent test conditions

## 📋 Quick Reference

### Common Configurations

```bash
# Development
--batch-interval 1.0 --batch-size 50 --threads 1

# Testing  
--batch-interval 0.1 --batch-size 100 --threads 2

# Performance
--batch-interval 0.01 --batch-size 500 --threads 4

# Stress
--batch-interval 0 --batch-size 1000 --threads 8

# Extreme
--batch-interval 0 --batch-size 2000 --threads 16
```

### Performance Estimation Formula

```
Theoretical Max Records/Second = 
  (Threads × Batch Size) / (Batch Interval + Database Response Time)

Example:
  Threads: 4
  Batch Size: 500  
  Interval: 0.01
  DB Response: 0.005 seconds
  
  Max = (4 × 500) / (0.01 + 0.005) = 133,333 records/second
```

Remember: **Actual performance will be limited by your database capacity!**

## 🎯 Best Practices

1. **Start Small**: Begin with conservative settings
2. **Measure Everything**: Monitor both client and database metrics
3. **Test Incrementally**: Gradually increase load pressure
4. **Consider Your Use Case**: Match testing to production patterns
5. **Document Results**: Keep records of what works for your setup
6. **Plan for Failures**: Test error handling and recovery
7. **Think Long-term**: Consider sustained vs. burst performance

Happy testing! 🚀