package com.sam.enrichment.processor;

import com.hortonworks.streamline.cache.Cache;
import com.hortonworks.streamline.cache.stats.CacheStats;
import com.hortonworks.streamline.cache.view.config.ExpiryPolicy;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryCache
  implements Cache<String, Object>
{
  Map<String, Object> myMap = new ConcurrentHashMap<String,Object>();
  
  public void clear()
  {
    this.myMap.clear();
  }
  
  public Object get(String key)
  {
    return this.myMap.get(key);
  }
  
  public Map<String, Object> getAll(Collection<? extends String> arg0)
  {
    return this.myMap;
  }
  
  public ExpiryPolicy getExpiryPolicy()
  {
    return null;
  }
  
  public void put(String key, Object val)
  {
    this.myMap.put(key, val);
  }
  
  public void putAll(Map<? extends String, ? extends Object> arg0) {}
  
  public void remove(String arg0) {}
  
  public void removeAll(Collection<? extends String> arg0) {}
  
  public long size()
  {
    return 0L;
  }
  
  public CacheStats stats()
  {
    return null;
  }
}
