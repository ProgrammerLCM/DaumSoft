package newcomer_task1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import data.DataDAO;

public class Perform3 {
	
	public Perform3() {	}

	public Perform3(int batchSize) {
		action1_1(batchSize);
	}

	public void action1_1(int batch) {
		
		long start = System.currentTimeMillis();
		
		String filepath = "C:\\Users\\Daumsoft\\eclipse-workspace\\newcomer_task1\\perform2_tag.txt";
		//String filepath = "C:\\Users\\Daumsoft\\eclipse-workspace\\newcomer_task1\\perform3_json.json";
		FileInputStream fileInputStream = null;
		InputStreamReader inputStreamReader = null;
		BufferedReader bufferedReader = null;
		String str = null;
		boolean flag = false;
		String[] strarr = null;
		List<String> textColumnList = null;
		List<String[]> list = null;
		int arrsize = 0;
		int i = 0;
		DataDAO dao = new DataDAO("2");
		
		try {
			fileInputStream = new FileInputStream(filepath);
			inputStreamReader = new InputStreamReader(fileInputStream,"UTF-8");
			bufferedReader = new BufferedReader(inputStreamReader);
			
			//DBColumnList = dao.selectColumn();
			textColumnList = getTextColumnList(filepath);
			arrsize = textColumnList.size();
			
			list = new ArrayList<String[]>();
			while((str = bufferedReader.readLine()) != null) {
				if(str.charAt(0)=='^' && !flag) {	// start
					flag = true;
					strarr = new String[arrsize];
				} else if(str.charAt(0)=='^' && flag) {	// end
					list.add(strarr);
					flag = false;
					i = 0;
				} else if(str.charAt(0)!='['){
					strarr[i] = str;
					i++;
				}
			}
			
			dao.insert_inputLbatchOfor(list, batch, textColumnList);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("Perform3_action1_1 실행완료!!!");
		System.out.println("처리시간 : " + (end-start));
		
	}

	private List<String> getTextColumnList(String filepath) {
		
		FileInputStream fileInputStream = null;
		InputStreamReader inputStreamReader = null;
		BufferedReader bufferedReader = null;
		
		boolean flag = false;
		String str = null;
		List<String> result = null;
		
		try {
			fileInputStream = new FileInputStream(filepath);
			inputStreamReader =new InputStreamReader(fileInputStream,"UTF8");
			bufferedReader = new BufferedReader(inputStreamReader);
			
			/*
				SELECT TITLE FROM DOC1 WHERE TITLE LIKE '[%]';
				'['로 시작하고 ']'로 끝나는 TITLE이 있음
				
				SELECT TITLE FROM DOC1 WHERE TITLE LIKE '^%';
				'^'로 시작하는 TITLE이 있음
				
				조건 다시 변경
			*/
			/* 기존 코드
			while((str = bufferedReader.readLine()) != null) {
				if(str.charAt(0)=='^' && !flag) {	// start
					flag = true;
					result = new ArrayList<String>();
				} else if(str.charAt(0)=='[') {
					result.add(str.substring(1,str.length()-1));
				} else if(str.charAt(0)=='^' && flag) {	// end
					break;
				}
			}
			*/
			while((str = bufferedReader.readLine()) != null) {
				if(str.charAt(0)=='^' && !flag) {	// start
					flag = true;
					result = new ArrayList<String>();
				} else if(str.charAt(0)=='[') {
					result.add(str.substring(1,str.length()-1));
				} else if(str.charAt(0)=='^' && flag) {	// end
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(bufferedReader != null) { try { bufferedReader.close(); } catch (IOException e) { } }
			if(inputStreamReader != null) { try { inputStreamReader.close(); } catch (IOException e) { } }
			if(fileInputStream != null) { try { fileInputStream.close(); } catch (IOException e) { } }
		}
		
		return result;
	}
	
}
