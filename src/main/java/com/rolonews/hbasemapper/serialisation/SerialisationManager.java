package com.rolonews.hbasemapper.serialisation;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 
 * @author maamria
 *
 */
public final class SerialisationManager {

	private static final Logger log = LoggerFactory.getLogger(SerialisationManager.class);

	private final Map<Class<?>, ObjectSerializer<?>> serialisers;
	
	private final BasicObjectSerializer basicObjectSerializer;
	
	public SerialisationManager(Map<Class<?>, ObjectSerializer<?>> serialisers){
		this.serialisers = serialisers;
		this.basicObjectSerializer = new BasicObjectSerializer();
	}
	
	public <T> byte[] serialize(T object){
		Preconditions.checkNotNull(object);
		Class<?> clazz = object.getClass();
		log.debug("Serialising object of class {}", clazz);
		if(basicObjectSerializer.canSerialize(clazz)){
			log.debug("Serialising object of class {} using basic serialiser", clazz);
			return basicObjectSerializer.serialize(object);
		}
		ObjectSerializer<T> objectSerializer = (ObjectSerializer<T>) serialisers.get(clazz);
		if(objectSerializer != null){
			log.debug("Serialising object of class {} using custom serilaiser", clazz);
			return objectSerializer.serialize(object, this);
		}
		log.warn("Serialising object of class {} not implemented or not registered", clazz);
		throw new NotImplementedException();
	}
	
	public <T> T deserialize(byte[] bytes, Class<T> targetType){
		Preconditions.checkNotNull(bytes);
		Preconditions.checkNotNull(targetType);
		log.debug("Deserialising to object of class {}", targetType);
		if(basicObjectSerializer.canSerialize(targetType)){
			log.debug("Deserialising to object of class {} using basic deserilaiser", targetType);
			return basicObjectSerializer.deserialize(bytes, targetType);
		}
		ObjectSerializer<T> objectSerializer = (ObjectSerializer<T>) serialisers.get(targetType);
		if(objectSerializer != null){
			log.debug("Deserialising to object of class {} using custom deserilaiser", targetType);
			return objectSerializer.deserialize(bytes, targetType, this);
		}
		log.warn("Deserialising to object of class {} not implemented or not registered", targetType);
		throw new NotImplementedException();
	}
}
