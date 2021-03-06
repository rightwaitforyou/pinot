package com.linkedin.thirdeye.detector.db;

import io.dropwizard.hibernate.AbstractDAO;

import java.util.List;

import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.linkedin.thirdeye.detector.api.AnomalyResult;

public class AnomalyResultDAO extends AbstractDAO<AnomalyResult> {
  public AnomalyResultDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public AnomalyResult findById(Long id) {
    return get(id);
  }

  public Long create(AnomalyResult anomalyResult) {
    return persist(anomalyResult).getId();
  }

  public void delete(Long id) {
    AnomalyResult anomalyResult = new AnomalyResult();
    anomalyResult.setId(id);
    currentSession().delete(id);
  }

  public List<AnomalyResult> findAllByCollectionAndTime(String collection, DateTime startTime,
      DateTime endTime) {
    return list(namedQuery("com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionAndTime")
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis()));
  }

  public List<AnomalyResult> findAllByCollectionTimeAndMetric(String collection, String metric,
      DateTime startTime, DateTime endTime) {
    return list(namedQuery(
        "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeAndMetric")
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("metric", metric));
  }

  public List<AnomalyResult> findAllByCollectionTimeFunctionIdAndMetric(String collection,
      String metric, long functionId, DateTime startTime, DateTime endTime) {
    return list(namedQuery(
        "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeFunctionIdAndMetric")
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("functionId", functionId).setParameter("metric", metric));
  }

  public List<AnomalyResult> findAllByCollectionTimeAndFunction(String collection,
      DateTime startTime, DateTime endTime, long functionId) {
    return list(namedQuery(
        "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeAndFunction")
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("functionId", functionId));
  }

  public List<AnomalyResult> findAllByCollectionTimeMetricAndFilters(String collection,
      String metric, DateTime startTime, DateTime endTime, String filters) {
    return list(namedQuery(
        "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeMetricAndFilters")
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("metric", metric).setParameter("filters", filters));
  }

  public List<AnomalyResult> findAllByCollectionTimeAndFilters(String collection,
      DateTime startTime, DateTime endTime, String filters) {
    return list(namedQuery(
        "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeAndFilters")
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("filters", filters));
  }
}
