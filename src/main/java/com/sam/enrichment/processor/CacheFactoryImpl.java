package com.sam.enrichment.processor;

import com.hortonworks.streamline.cache.Cache;
import com.hortonworks.streamline.streams.runtime.CacheBackedProcessorRuntime.CacheFactory;

public class CacheFactoryImpl
  implements CacheFactory<String,Object>
{
  public Cache<String, Object> create()
  {
    return new InMemoryCache();
  }
}
