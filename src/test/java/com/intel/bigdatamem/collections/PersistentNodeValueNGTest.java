package com.intel.bigdatamem.collections;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.intel.bigdatamem.BigDataPMemAllocator;
import com.intel.bigdatamem.CommonPersistAllocator;
import com.intel.bigdatamem.Durable;
import com.intel.bigdatamem.EntityFactoryProxy;
import com.intel.bigdatamem.GenericField;
import com.intel.bigdatamem.Reclaim;
import com.intel.bigdatamem.Utils;

/**
 *
 * @author Wang, Gang {@literal <gang1.wang@intel.com>}
 *
 */


public class PersistentNodeValueNGTest {
	private long KEYCAPACITY;
	private Random m_rand;
	private BigDataPMemAllocator m_act;

	@BeforeClass
	public void setUp() {
		m_rand = Utils.createRandom();
		m_act = new BigDataPMemAllocator(1024 * 1024 * 1024, "./pobj_NodeValue.dat", true);
		KEYCAPACITY = m_act.persistKeyCapacity();
		m_act.setBufferReclaimer(new Reclaim<ByteBuffer>() {
			@Override
			public boolean reclaim(ByteBuffer mres, Long sz) {
				System.out.println(String.format("Reclaim Memory Buffer: %X  Size: %s", System.identityHashCode(mres),
						null == sz ? "NULL" : sz.toString()));
				return false;
			}
		});
		m_act.setChunkReclaimer(new Reclaim<Long>() {
			@Override
			public boolean reclaim(Long mres, Long sz) {
				System.out.println(String.format("Reclaim Memory Chunk: %X  Size: %s", System.identityHashCode(mres),
						null == sz ? "NULL" : sz.toString()));
				return false;
			}
		});
		
		for (long i = 0; i < KEYCAPACITY; ++i) {
			m_act.setPersistKey(i, 0L);
		}
	}
	
	@AfterClass
	public void tearDown() {
		m_act.close();
	}

	@Test(enabled = false)
	public void testSingleNodeValueWithInteger() {
		int val = m_rand.nextInt();
		GenericField.GType gtypes[] = {GenericField.GType.INTEGER}; 
		PersistentNodeValue<Integer> plln = PersistentNodeValueFactory.create(m_act, null, gtypes, false);
		plln.setItem(val, false);
		Long handler = plln.getPersistentHandler();
		System.err.println("-------------Start to Restore Integer -----------");
		PersistentNodeValue<Integer> plln2 = PersistentNodeValueFactory.restore(m_act, null, gtypes, handler, false);
		AssertJUnit.assertEquals(val, (int)plln2.getItem());
	}
	
	@Test(enabled = false)
	public void testNodeValueWithString() {
		String val = Utils.genRandomString();
		GenericField.GType gtypes[] = {GenericField.GType.STRING}; 
		PersistentNodeValue<String> plln = PersistentNodeValueFactory.create(m_act, null, gtypes, false);
		plln.setItem(val, false);
		Long handler = plln.getPersistentHandler();
		System.err.println("-------------Start to Restore String-----------");
		PersistentNodeValue<String> plln2 = PersistentNodeValueFactory.restore(m_act, null, gtypes, handler, false);
		AssertJUnit.assertEquals(val, plln2.getItem());
	}
	
	@Test(enabled = false)
	public void testNodeValueWithPerson() {

		Person<Long> person = PersonFactory.create(m_act);
		person.setAge((short)31);
		
		GenericField.GType gtypes[] = {GenericField.GType.DURABLE}; 
		EntityFactoryProxy efproxies[] = {new EntityFactoryProxy(){
			@Override
			public <A extends CommonPersistAllocator<A>> Durable restore(A allocator,
					EntityFactoryProxy[] factoryproxys, GenericField.GType[] gfields, long phandler, boolean autoreclaim) {
				return PersonFactory.restore(allocator, factoryproxys, gfields, phandler, autoreclaim);
			    }
			}
		};
		
		PersistentNodeValue<Person<Long>> plln = PersistentNodeValueFactory.create(m_act, efproxies, gtypes, false);
		plln.setItem(person, false);
		Long handler = plln.getPersistentHandler();
		
		PersistentNodeValue<Person<Long>> plln2 = PersistentNodeValueFactory.restore(m_act, efproxies, gtypes, handler, false);
		AssertJUnit.assertEquals(31, (int)plln2.getItem().getAge());

	}
	@Test(enabled = false)
	public void testLinkedNodeValueWithPerson() {

		int elem_count = 10;
		List<Long> referlist = new ArrayList();

		GenericField.GType listgftypes[] = {GenericField.GType.DURABLE}; 
		EntityFactoryProxy listefproxies[] = { 
				new EntityFactoryProxy(){
						@Override
						public <A extends CommonPersistAllocator<A>> Durable restore(A allocator,
								EntityFactoryProxy[] factoryproxys, GenericField.GType[] gfields, long phandler, boolean autoreclaim) {
							return PersonFactory.restore(allocator, factoryproxys, gfields, phandler, autoreclaim);
						    }
						}
				};
		
		PersistentNodeValue<Person<Long>> firstnv = PersistentNodeValueFactory.create(m_act, listefproxies, listgftypes, false);
		
		PersistentNodeValue<Person<Long>> nextnv = firstnv;
		
		Person<Long> person;
		long val;
		PersistentNodeValue<Person<Long>> newnv;
		for (int i = 0; i < elem_count; ++i) {
			person = PersonFactory.create(m_act);
			person.setAge((short)m_rand.nextInt(50));
			person.setName(String.format("Name: [%s]", Utils.genRandomString()), true);
			nextnv.setItem(person, false);
			newnv = PersistentNodeValueFactory.create(m_act, listefproxies, listgftypes, false);
			nextnv.setNext(newnv, false);
			nextnv = newnv;
		}
		
		Person<Long> eval;
		PersistentNodeValue<Person<Long>> iternv = firstnv;
		while(null != iternv) {
			System.out.printf(" Stage 1 --->\n");
		    eval = iternv.getItem();
			if (null != eval)
				eval.testOutput();
			iternv = iternv.getNext();
		}
		
		long handler = firstnv.getPersistentHandler();
		
		PersistentNodeValue<Person<Long>> firstnv2 = PersistentNodeValueFactory.restore(m_act, listefproxies, listgftypes, handler, false);
		
		for (Person<Long> eval2 : firstnv2) {
			System.out.printf(" Stage 2 ---> \n");
			if (null != eval2)
				eval2.testOutput();
		}
		
		//Assert.assert, expected);(plist, plist2);
		
	}
	
	@Test(enabled = true)
	public void testLinkedNodeValueWithLinkedNodeValue() {

		int elem_count = 10;
		long slotKeyId = 10;

		GenericField.GType[] elem_gftypes = {GenericField.GType.DOUBLE};
        EntityFactoryProxy[] elem_efproxies = null;

		GenericField.GType linkedgftypes[] = {GenericField.GType.DURABLE, GenericField.GType.DOUBLE};
		EntityFactoryProxy linkedefproxies[] = { 
				new EntityFactoryProxy(){
						@Override
						public <A extends CommonPersistAllocator<A>> Durable restore(A allocator,
								EntityFactoryProxy[] factoryproxys, GenericField.GType[] gfields, long phandler, boolean autoreclaim) {
								EntityFactoryProxy[] val_efproxies = null;
								GenericField.GType[] val_gftypes = null;
								if ( null != factoryproxys && factoryproxys.length >= 2 ) {
									val_efproxies = Arrays.copyOfRange(factoryproxys, 1, factoryproxys.length);
								}
								if ( null != gfields && gfields.length >= 2 ) {
									val_gftypes = Arrays.copyOfRange(gfields, 1, gfields.length);
								}
								return PersistentNodeValueFactory.restore(allocator, val_efproxies, val_gftypes, phandler, autoreclaim);
						    }
						}
				};
		
		PersistentNodeValue<PersistentNodeValue<Double>> nextnv = null, pre_nextnv = null;
		PersistentNodeValue<Double> elem = null, pre_elem = null, first_elem = null;
		
		Long linkhandler = 0L;
		
		System.out.printf(" Stage 1 -testLinkedNodeValueWithLinkedNodeValue--> \n");

		pre_nextnv = null;
		Double val;
		for (int i=0; i< elem_count; ++i) {
			first_elem = null;
			pre_elem = null;
			for (int v=0; v<3 ; ++v) {
				elem = PersistentNodeValueFactory.create(m_act, elem_efproxies, elem_gftypes, false);
				val = m_rand.nextDouble();
				elem.setItem(val, false);
				if (null == pre_elem) {
					first_elem = elem;
				} else {
					pre_elem.setNext(elem, false);
				}
				pre_elem = elem;
				System.out.printf("%f ", val);
			}
			
			nextnv = PersistentNodeValueFactory.create(m_act, linkedefproxies, linkedgftypes, false);
			nextnv.setItem(first_elem, false);
			if (null == pre_nextnv) {
				linkhandler = nextnv.getPersistentHandler();
			} else {
				pre_nextnv.setNext(nextnv, false);
			}
			pre_nextnv = nextnv;
			System.out.printf(" generated an item... \n");
		}
		m_act.setPersistKey(slotKeyId, linkhandler);
		
		long handler = m_act.getPersistKey(slotKeyId);
		
		PersistentNodeValue<PersistentNodeValue<Double>> linkedvals = PersistentNodeValueFactory.restore(m_act, linkedefproxies, linkedgftypes, handler, false);
		Iterator<PersistentNodeValue<Double>> iter = linkedvals.iterator();
		Iterator<Double> elemiter = null;
		
		System.out.printf(" Stage 2 -testLinkedNodeValueWithLinkedNodeValue--> \n");
		while(iter.hasNext()) {
			elemiter = iter.next().iterator();
			while(elemiter.hasNext()) {
				System.out.printf("%f ", elemiter.next());
			}
			System.out.printf(" Fetched an item... \n");
		}
		
		//Assert.assert, expected);(plist, plist2);
		
	}

}
