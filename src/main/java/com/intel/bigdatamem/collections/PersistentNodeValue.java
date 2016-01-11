package com.intel.bigdatamem.collections;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.intel.bigdatamem.*;

/**
 *
 *
 */


@PersistentEntity
public abstract class PersistentNodeValue<E> 
    implements Durable, Iterable<E> {
	protected transient EntityFactoryProxy[] m_node_efproxies;
	protected transient GenericField.GType[] m_node_gftypes;
	
	@Override
	public void initializeAfterCreate() {
//		System.out.println("Initializing After Created");
	}

	@Override
	public void initializeAfterRestore() {
//		System.out.println("Initializing After Restored");
	}
	
	@Override
	public void setupGenericInfo(EntityFactoryProxy[] efproxies, GenericField.GType[] gftypes) {
		m_node_efproxies = efproxies;
		m_node_gftypes = gftypes;
	}
	
	@PersistentGetter(EntityFactoryProxies = "m_node_efproxies", GenericFieldTypes = "m_node_gftypes")
	abstract public E getItem();
	@PersistentSetter
	abstract public void setItem(E next, boolean destroy);
	
	@PersistentGetter(EntityFactoryProxies = "m_node_efproxies", GenericFieldTypes = "m_node_gftypes")
	abstract public PersistentNodeValue<E> getNext();
	@PersistentSetter
	abstract public void setNext(PersistentNodeValue<E> next, boolean destroy);
	
	
	
	@Override
	public Iterator<E> 	iterator() {
		return new Intr(this);
	}
	
	private class Intr implements Iterator<E> {
		
		protected PersistentNodeValue<E> next = null;
		
		Intr(PersistentNodeValue<E> head) {
			next = head;
		}
		
		@Override
		public boolean hasNext() {
			return null != next;
		}
		
		@Override
		public E next() {
			if (null == next) {
				new NoSuchElementException();
			}
			E ret = next.getItem();
			next = next.getNext();
			return ret;
		}
	}
}
