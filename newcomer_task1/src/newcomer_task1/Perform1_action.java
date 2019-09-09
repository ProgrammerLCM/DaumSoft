package newcomer_task1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import data.DataDAO;


/*
	InputStreamReader / OutputStreamWriter를 같이 사용하여 버퍼링을 하게 되면 입출력 스트림으로부터 미리 버퍼에 데이터를 갖다 놓기 때문에 보다 효율적인 입출력이 가능 
*/

public class Perform1_action {
	
	private String filepath = null;
	private DataDAO dao = null;
	private FileInputStream fileInputStream = null;
	private InputStreamReader inputStreamReader = null;
	private BufferedReader bufferedReader = null;
	private List<String> colist = null;

	public Perform1_action() {
		filepath = "C:\\Users\\Daumsoft\\git\\DaumSoft\\newcomer_task1\\doc.tsv";
		dao = new DataDAO("1");
		colist = dao.selectColumn();
	}
	
	public long inputBbyONEbatchX() {	// 입력버퍼를 전달, 한줄한줄 읽으며 삽입, batch 미사용

		long start = System.currentTimeMillis();

		try {
			fileInputStream = new FileInputStream(filepath);
			inputStreamReader =new InputStreamReader(fileInputStream,"UTF8");
			bufferedReader = new BufferedReader(inputStreamReader);

			dao.insert_inputBbyONEbatchX(bufferedReader, colist);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
			
		long end = System.currentTimeMillis();
		
		//System.out.println("inputBbyONEbatchX 실행완료!!!");
		//System.out.print((end-start) + "\t");
		
		return end-start;
		
	}
	

	public long inputLbyONEbatchX() {	// 리스트를 전달, 한줄한줄 읽으며 삽입, batch 미사용
		
		long start = System.currentTimeMillis();
		
		TsvParserSettings settings = new TsvParserSettings();
		TsvParser parser = new TsvParser(settings);
		List<String[]> list = null;
		
		try {
			fileInputStream = new FileInputStream(filepath);
			inputStreamReader =new InputStreamReader(fileInputStream,"UTF8");
			bufferedReader = new BufferedReader(inputStreamReader);

			settings = new TsvParserSettings();
			parser = new TsvParser(settings);
			list = parser.parseAll(bufferedReader);
			
			dao.insert_inputLbyONEbatchX(list, colist);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
		
		long end = System.currentTimeMillis();
		
		//System.out.println("inputLbyONEbatchX 실행완료!!!");
		//System.out.print((end-start) + "\t");
		
		return end-start;
		
	}

	public long inputBbyONEbatchO(int batch) {	// 입력버퍼를 전달, 한줄한줄 읽으며 삽입, batch 사용
		
		long start = System.currentTimeMillis();
		
		try {
			fileInputStream = new FileInputStream(filepath);
			inputStreamReader =new InputStreamReader(fileInputStream,"UTF8");
			bufferedReader = new BufferedReader(inputStreamReader);

			dao.insert_inputBbyONEbatchO(bufferedReader, batch, colist);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
		
		long end = System.currentTimeMillis();
		
		//System.out.println("inputBbyONEbatchO 실행완료!!!");
		//System.out.print((end-start) + "\t");
		
		return end-start;
		
	}
	
	
	public long inputLbatchObasic(int batch) {	// 전체를 파싱하고 리스트 형태로 전달, batch을 사용해서 삽입, 기본 batch
		
		long start = System.currentTimeMillis();
		
		TsvParserSettings settings = new TsvParserSettings();
		TsvParser parser = new TsvParser(settings);
		List<String[]> list = null;

		try {
			// FileInputStream : 파일을 바이트 단위로 읽어옴
			// InputStreamReader : 바이트 스트림을 문자 스트림으로 변환
			fileInputStream = new FileInputStream(filepath);
			inputStreamReader = new InputStreamReader(fileInputStream,"UTF8");
			bufferedReader = new BufferedReader(inputStreamReader);

			settings = new TsvParserSettings();
			parser = new TsvParser(settings);
			list = parser.parseAll(bufferedReader);

			dao.insert_inputLbatchObasic(list, batch, colist);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
		
		long end = System.currentTimeMillis();
		
		//System.out.println("inputLbatchObasic 실행완료!!!");
		//System.out.print((end-start) + "\t");
		
		return end-start;
	}

	
	public long inputLbatchOfor(int batch) {	// 전체를 파싱하고 리스트 형태로 전달, batch을 사용해서 삽입, 이중 for문 batch
		
		long start = System.currentTimeMillis();
	
		TsvParserSettings settings = new TsvParserSettings();
		TsvParser parser = new TsvParser(settings);
		List<String[]> list = null;
		
		try {
			// FileInputStream : 파일을 바이트 단위로 읽어옴
			// InputStreamReader : 바이트 스트림을 문자 스트림으로 변환
			fileInputStream = new FileInputStream(filepath);
			inputStreamReader = new InputStreamReader(fileInputStream,"UTF8");
			bufferedReader = new BufferedReader(inputStreamReader);
			
			settings = new TsvParserSettings();
			parser = new TsvParser(settings);
			list = parser.parseAll(bufferedReader);
			
			dao.insert_inputLbatchOfor(list, batch, colist, "skip");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
		
		long end = System.currentTimeMillis();
		
		//System.out.println("inputLbatchOfor 실행완료!!!");
		//System.out.print((end-start) + "\t");
		
		return end-start;
	}
}
