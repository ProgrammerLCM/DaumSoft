package newcomer_task1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.json.simple.JSONObject;

import data.DataDAO;
import util.DBConnection;

public class Perform2 {
	
	public Perform2() {
		action1();
		action2();
		action3();
	}

	public void action1() {		// tsv파일로 오름차순, 내림차순 tsv형식파일 생성
		
		long start = System.currentTimeMillis();
		
		List<String[]> list = null;
		List<String> column = null;
		Connection conn = new DBConnection().getConnection();
		DataDAO dao = new DataDAO("1");
		
		column = dao.selectColumn();

		list = dao.selectList("asc");
		fileWrite(column, list, "perform1_1", "tsv", "tsv");
		
		list = dao.selectList("desc");
		fileWrite(column, list, "perform1_2", "tsv", "tsv");
		
		if (conn != null) try { conn.close(); } catch(SQLException ex) {}
		
		long end = System.currentTimeMillis();
		
		System.out.println("Perform2_action1(tsv파일) 실행완료!!!");
		System.out.println("처리시간 : " + (end-start));
		
	}
	
	public void action2() {		// txt파일로 tag형식파일 생성
		
		long start = System.currentTimeMillis();
		
		List<String[]> list = null;
		List<String> column = null;
		Connection conn = new DBConnection().getConnection();
		DataDAO dao = new DataDAO("1");
		
		column = dao.selectColumn();

		list = dao.selectList("none");
		fileWrite(column, list, "perform2", "tag", "txt");
		
		if (conn != null) try { conn.close(); } catch(SQLException ex) {}

		long end = System.currentTimeMillis();
		
		System.out.println("Perform2_action2(tag파일) 실행완료!!!");
		System.out.println("처리시간 : " + (end-start));
		
	}
	
	public void action3() {		// json파일로 json형식파일 생성

		long start = System.currentTimeMillis();
		
		List<String[]> list = null;
		List<String> column = null;
		Connection conn = new DBConnection().getConnection();
		DataDAO dao = new DataDAO("1");
		
		column = dao.selectColumn();
		
		list = dao.selectList("none");
		fileWrite(column, list, "perform3", "json", "json");
		
		if (conn != null) try { conn.close(); } catch(SQLException ex) {}

		long end = System.currentTimeMillis();
		
		System.out.println("Perform2_action3(json파일) 실행완료!!!");
		System.out.println("처리시간 : " + (end-start) + "\n");
		
	}
	
	public void fileWrite(List<String> column, List<String[]> list, String sub, String type, String extension) {

		String filepath = "C:\\Users\\Daumsoft\\git\\DaumSoft\\newcomer_task1";
		String title = sub + "_" + type;
		
		try {
			BufferedWriter fw = new BufferedWriter(new FileWriter(filepath+"/"+title+"."+extension, false));
			
			if(type.equals("tsv")) {
				fw.write(column.get(0)+"\t"+column.get(1)+"\t"+column.get(2));
				fw.newLine();
				for (String[] str : list) {
					fw.write(str[0]+"\t"+str[1]+"\t"+str[2]);
					fw.newLine();
				}
			} else if(type.equals("tag")) {
				for (String[] str : list) {
					fw.write("^[START]\r\n[" + column.get(0) + "]\r\n" + str[0] + "\r\n[" + column.get(1) + "]\r\n" + str[1] + "\r\n[" + column.get(2) + "]\r\n" + str[2]+"\r\n^[END]");
					fw.newLine();
				}
			} else if(type.equals("json")) {
				for(String[] str : list) {
					JSONObject jsonObject = new JSONObject();	// 최종 완성될 JSONObject 선언(전체)
					jsonObject.put(column.get(0), str[0]);
					jsonObject.put(column.get(1), str[1]);
					jsonObject.put(column.get(2), str[2]);
					//System.out.println(jsonObject.toString());
					//System.out.println(jsonObject.toJSONString());
					fw.write(jsonObject.toString());
					fw.newLine();
				}
			}
			fw.flush();		// 남아있는 데이터를 모두 출력시킴
			fw.close();		// 스트림을 닫음
		} catch ( Exception e) {
			e.printStackTrace();
		}
	}
}
