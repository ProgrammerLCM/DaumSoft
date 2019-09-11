package newcomer_task1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import data.DataDAO;

public class Perform1 {
	
	private DataDAO dao = null;
	private FileInputStream fileInputStream = null;
	private InputStreamReader inputStreamReader = null;
	private BufferedReader bufferedReader = null;
	private String[] methodCommentSet = {"버퍼를 통해 한 줄씩, batch 미사용", "리스트를 전달 한줄씩, batch 미사용", "버퍼를 통해 한 줄씩, batch 사용", "리스트를 전달, batch 사용(기본)", "리스트를 전달, batch 사용(이중for문)"};
	private String[] methodNameSet = {"inputBbyONEbatchX", "inputLbyONEbatchX", "inputBbyONEbatchO", "inputLbatchObasic", "inputLbatchOfor"};  
	private int[] batchSizeSet = {10, 100, 1000, 5000, 10000, 50000, 100000, 175800};
	private int loopCount = 1;
	private long[] resultSet = null;
	private long average = 0;
	Class cls = null;
	Object obj = null;
	Method method = null;
	
	// 테이블 하나 채울 때 쓰는 생성자
	public Perform1(int batchSize) {
		//inputOneSQL(batchSize);
		inputOneSQL2(batchSize);
	}
	
	public Perform1() {
		
		//no paramater
		Class noparams[] = {};
			
		//int parameter
		Class[] paramInt = new Class[1];	
		paramInt[0] = Integer.TYPE;
		
		try {
			cls = Class.forName("newcomer_task1.Perform1_action");
			obj = cls.newInstance();
			resultSet = new long[loopCount + 1];
			
			System.out.println("File to DB");
			System.out.println("==========");
			
			for(int i=0; i<methodNameSet.length; i++) {
				System.out.println(methodCommentSet[i]);
				if(i<2) {
					for(int k=0; k<loopCount; k++) {
						method = cls.getDeclaredMethod(methodNameSet[i], noparams);
						resultSet[k] = (long)method.invoke(obj, null);
						average += resultSet[k];
						if(countData()==175799)
							clearDB();
						else
							throw new ArithmeticException();
					}
					printResult(resultSet, average/loopCount);
					init();
					System.out.println("\n");
				} else {
					for(int j=0; j<batchSizeSet.length; j++) {
						System.out.println(batchSizeSet[j]);
						for(int k=0; k<loopCount; k++) {
							method = cls.getDeclaredMethod(methodNameSet[i], paramInt);
							resultSet[k] = (long)method.invoke(obj, batchSizeSet[j]);
							average += resultSet[k];
							if(countData()==175799)
								clearDB();
							else
								throw new ArithmeticException();
						}
						printResult(resultSet, average/loopCount);
						init();
						System.out.println();
					}
				}
			}
			
		} catch (IllegalAccessException e) {
			//log.error("Illiegal Access Exception", e);
			System.out.println("Illiegal Access Exception");
			
		// InvocationTargetException는 method invoke시 호출한 메소드 내에서 Exception이 발생했을때 해당 wrapping 해주는 Exception 클래스
		// 발생한 예외는 invoke 구문에서 발생한것처럼 보이기 때문에 InvocationTargetException 자체의 stack trace 만으로 에러를 해결하기가 어렵다.
		// 따라서 Exception으로 예외를 처리하는게 아니라 아래와 같이 예외처리를 해줘야 한다.
		} catch (InvocationTargetException e) { 					
			//log.error("fail to invoke method", e);
			System.out.println("fail to invoke method");
		} catch (ArithmeticException e) {
			System.out.println("There is not enough data in the database.");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
		
	}
	
	private int countData() {
		dao = new DataDAO();
		//int a = dao.countData();
		//System.out.println("datacount : " + a);
		//return a;
		return dao.countData();
	}
	
	private void printResult(long[] resultSet, long average) {
		for(int i=0; i<resultSet.length; i++)
			System.out.print(resultSet[i] + " ");
		System.out.println("\t" + average);
	}
	
	private void clearDB() {
		dao.clearDB();
	}
	
	private void init() {
		for(int i=0; i<loopCount; i++) {
			resultSet[i] = 0;
		}
		average = 0;
	}
	
	
	
	
	
public long inputLbatchOfor(int batch) {	// 전체를 파싱하고 리스트 형태로 전달, batch을 사용해서 삽입, 이중 for문 batch
		
		long start = System.currentTimeMillis();
	
		TsvParserSettings settings = new TsvParserSettings();
		TsvParser parser = new TsvParser(settings);
		List<String[]> list = null;
		List<String> colist = null;
		
		String filepath = "C:\\Users\\Daumsoft\\git\\DaumSoft\\newcomer_task1\\doc.tsv";
		
		try {
			// FileInputStream : 파일을 바이트 단위로 읽어옴
			// InputStreamReader : 바이트 스트림을 문자 스트림으로 변환
			fileInputStream = new FileInputStream(filepath);
			inputStreamReader = new InputStreamReader(fileInputStream,"UTF8");
			bufferedReader = new BufferedReader(inputStreamReader);
			dao = new DataDAO("1");
			
			settings = new TsvParserSettings();
			parser = new TsvParser(settings);
			list = parser.parseAll(bufferedReader);
			colist = dao.selectColumn();
			dao.insert_inputLbatchOfor(list, batch, colist, "skip");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("inputLbatchOfor 실행완료!!!");
		System.out.println("처리시간 : " + (end-start) + "\n");
		
		return end-start;
	}





	public long inputOneSQL(int batch) {	// sql문 모아서 보내기
		
		long start = System.currentTimeMillis();
		
		TsvParserSettings settings = null;
		TsvParser parser = null;
		List<String[]> list = null;
		List<String> colist = null;
		
		String filepath = "C:\\Users\\Daumsoft\\git\\DaumSoft\\newcomer_task1\\doc.tsv";
		
		try {
			// FileInputStream : 파일을 바이트 단위로 읽어옴
			// InputStreamReader : 바이트 스트림을 문자 스트림으로 변환
			fileInputStream = new FileInputStream(filepath);
			inputStreamReader = new InputStreamReader(fileInputStream,"UTF8");
			bufferedReader = new BufferedReader(inputStreamReader);
			dao = new DataDAO("1");
			
			settings = new TsvParserSettings();
			parser = new TsvParser(settings);
			list = parser.parseAll(bufferedReader);
			colist = dao.selectColumn();
			dao.insert_inputOneSQL(list, batch, colist, "skip");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
		
		long end = System.currentTimeMillis();
		
		//System.out.println("inputOneSQL 실행완료!!!");
		//System.out.print((end-start) + "\t");
		
		return end-start;
	}
	
	public long inputOneSQL2(int batch) {	// sql문 모아서 보내기
		
		long start = System.currentTimeMillis();
		
		TsvParserSettings settings = null;
		TsvParser parser = null;
		List<String[]> list = null;
		List<String> colist = null;
		
		String filepath = "C:\\Users\\Daumsoft\\git\\DaumSoft\\newcomer_task1\\doc.tsv";
		
		try {
			// FileInputStream : 파일을 바이트 단위로 읽어옴
			// InputStreamReader : 바이트 스트림을 문자 스트림으로 변환
			fileInputStream = new FileInputStream(filepath);
			inputStreamReader = new InputStreamReader(fileInputStream,"UTF8");
			bufferedReader = new BufferedReader(inputStreamReader);
			dao = new DataDAO("1");
			
			settings = new TsvParserSettings();
			parser = new TsvParser(settings);
			list = parser.parseAll(bufferedReader);
			colist = dao.selectColumn();
			dao.insert_inputOneSQL2(list, batch, colist);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("inputOneSQL2 실행완료!!!");
		System.out.print((end-start) + "\t");
		
		return end-start;
	}

	
}





