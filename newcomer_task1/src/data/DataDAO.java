package data;

import java.io.BufferedReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import util.DBConnection;

public class DataDAO {
	
	String tablename = null;
	
	public DataDAO() {	}

	public DataDAO(String num) {
		tablename = "DOC" + num;
	}
	
	// 한줄씩 읽어서 한줄씩 삽입, batch x
	public void insert_inputBbyONEbatchX(BufferedReader reader, List<String> columnlist) {
		
		String str = null;
		String[] strarr = null;
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		
		List<String> columnList = null;
		
		if(columnlist != null) {
			columnList = columnlist;
		}
		
		String sql = "insert into " + tablename + "(" + columnList.get(0) + "," + columnList.get(1) + "," + columnList.get(2) + ") values (?,?,?)";
		
		try {
			conn = new DBConnection().getConnection();

			pstmt = conn.prepareStatement(sql);
			reader.readLine();
			while((str = reader.readLine()) != null) {
				strarr = str.split("\t");
				pstmt.setInt(1, Integer.parseInt(strarr[0]));
				pstmt.setString(2, strarr[1]);
				pstmt.setString(3, strarr[2]);
				pstmt.executeUpdate();	
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) { try { pstmt.close(); } catch(SQLException ex) {} }
			if (conn != null) { try { conn.close(); } catch(SQLException ex) {} }
		}
	}
	
	// 한줄씩 읽어서 여러개씩 삽입, batch o
	public void insert_inputBbyONEbatchO(BufferedReader reader, int batch, List<String> columnlist) {
		
		String str = null;
		String[] strarr = null;
		int i = 0;
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		
		List<String> columnList = null;
		
		if(columnlist != null) {
			columnList = columnlist;
		}
		
		String sql = "insert into " + tablename + "(" + columnList.get(0) + "," + columnList.get(1) + "," + columnList.get(2) + ") values (?,?,?)";
		
		try {
			conn = new DBConnection().getConnection();
			pstmt = conn.prepareStatement(sql);
			
			reader.readLine();
			while((str = reader.readLine()) != null) {
				strarr = str.split("\t");
				pstmt.setInt(1, Integer.parseInt(strarr[0]));
				pstmt.setString(2, strarr[1]);
				pstmt.setString(3, strarr[2]);
				pstmt.addBatch();			// addBatch에 담기
				pstmt.clearParameters();	// 파라미터 Clear
				i++;
				if(i%batch==0) {
					pstmt.executeBatch();	// Batch 실행
					pstmt.clearBatch();		// Batch 초기화
					conn.commit();			// 커밋
				}
			}
			pstmt.executeBatch();	// Batch 실행
			pstmt.clearBatch();		// Batch 초기화
			conn.commit();			// 커밋
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) { try { pstmt.close(); } catch(SQLException ex) {} }
			if (conn != null) { try { conn.close(); } catch(SQLException ex) {} }
		}
	}
		
	
	// 통쨰로 읽어서 한줄씩 삽입, batch x
	public void insert_inputLbyONEbatchX(List<String[]> list, List<String> columnlist) {
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		List<String> columnList = null;
		
		if(columnlist != null) {
			columnList = columnlist;
		}
		
		//String sql = "insert into " + tablename + " values (?,?,?)";
		String sql = "insert into " + tablename + "(" + columnList.get(0) + "," + columnList.get(1) + "," + columnList.get(2) + ") values (?,?,?)";
		
		try {
			conn = new DBConnection().getConnection();
			pstmt = conn.prepareStatement(sql);
			
			for(int i=1; i<list.size(); i++) {
				pstmt.setInt(1, Integer.parseInt(list.get(i)[0]));
				pstmt.setString(2, list.get(i)[1]);
				pstmt.setString(3, list.get(i)[2]);
				pstmt.executeUpdate();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) try { pstmt.close(); } catch(SQLException ex) {} 
			if (conn != null) try { conn.close(); } catch(SQLException ex) {}
		}
	}
	
	
	
	// 통째로 읽어서 여래개씩 삽입, batch o 기본
	public void insert_inputLbatchObasic(List<String[]> list, int batch, List<String> columnlist) {
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		
		List<String> columnList = null;
		
		if(columnlist != null) {
			columnList = columnlist;
		}
		
		String sql = "insert into " + tablename + "(" + columnList.get(0) + "," + columnList.get(1) + "," + columnList.get(2) + ") values (?,?,?)";
		int batchsize = 0;
		
		batchsize = batch;
		
		try {
			conn = new DBConnection().getConnection();
			pstmt = conn.prepareStatement(sql);
			conn.setAutoCommit(false);
			
			for(int i=1; i<list.size(); i++) {
				pstmt.setInt(1, Integer.parseInt(list.get(i)[0]));
				pstmt.setString(2, list.get(i)[1]);
				pstmt.setString(3, list.get(i)[2]);
				pstmt.addBatch();			// addBatch에 담기
				pstmt.clearParameters();	// 파라미터 Clear
				
				if(i%batchsize==0) {
					pstmt.executeBatch();	// Batch 실행
					pstmt.clearBatch();		// Batch 초기화
					conn.commit();			// 커밋
				}
			}
			pstmt.executeBatch();	// Batch 실행
			pstmt.clearBatch();		// Batch 초기화
			conn.commit();			// 커밋
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) try { pstmt.close(); } catch(SQLException ex) {} 
			if (conn != null) try { conn.close(); } catch(SQLException ex) {}
		}
	}
	
	
	// 통째로 읽어서 여래개씩 삽입, batch o 이중 for문
	// https://fruitdev.tistory.com/111
	public void insert_inputLbatchOfor(List<String[]> list, int batch, List<String> columnlist, String type) {
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		
		List<String> columnList = null;
		
		if(columnlist != null) {
			columnList = columnlist;
		}
		
		String sql = "insert into " + tablename + "(" + columnList.get(0) + "," + columnList.get(1) + "," + columnList.get(2) + ") values (?,?,?)";
		int outloop = 0;
		int listsize = 0;
		int t = type.equals("skip")?1:0;
		listsize = list.size();
		outloop = (listsize-1)/batch;
		
		try {
			conn = new DBConnection().getConnection();
			pstmt = conn.prepareStatement(sql);
			conn.setAutoCommit(false);
			
			for(int i=0; i<outloop; i++) {
				for(int j=i*batch+t; j<=(i+1)*batch; j++) {
					pstmt.setInt(1, Integer.parseInt(list.get(j)[0]));
					pstmt.setString(2, list.get(j)[1]);
					pstmt.setString(3, list.get(j)[2]);
					pstmt.addBatch();			// addBatch에 담기
					pstmt.clearParameters();	// 파라미터 Clear
				}
                pstmt.executeBatch();	// Batch 실행
                pstmt.clearBatch();		// Batch 초기화
                conn.commit();			// 커밋
			}
			
			for(int i=outloop*batch+1; i<listsize; i++) {
				pstmt.setInt(1, Integer.parseInt(list.get(i)[0]));
				pstmt.setString(2, list.get(i)[1]);
				pstmt.setString(3, list.get(i)[2]);
				pstmt.addBatch();			// addBatch에 담기
				pstmt.clearParameters();	// 파라미터 Clear
			}
			pstmt.executeBatch();	// Batch 실행
            pstmt.clearBatch();		// Batch 초기화
            conn.commit();			// 커밋
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) try { pstmt.close(); } catch(SQLException ex) {} 
			if (conn != null) try { conn.close(); } catch(SQLException ex) {}
		}
	}
	//==================================================================
	
	
	public List<String[]> selectList(String type) {
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		String sql = "select * from " + tablename;
		String[] strarr = null;
		List<String[]> result = null;

		try {
			conn = new DBConnection().getConnection();
			result = new ArrayList<String[]>();
			
			if(type.equals("asc"))
				sql += " order by DOC_SEQ asc";
			else if(type.equals("desc"))
				sql += " order by DOC_SEQ desc";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			
			while(rs.next()) {
				strarr = new String[3];
				strarr[0] = Integer.toString(rs.getInt("DOC_SEQ"));
				strarr[1] = rs.getString("TITLE");
				strarr[2] = rs.getString("REG_DT");
				result.add(strarr);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) { try { rs.close(); } catch (SQLException e) {}}
			if (pstmt != null) { try { pstmt.close(); } catch(SQLException ex) {} }
			if (conn != null) { try { conn.close(); } catch(SQLException ex) {} }
		}
		
		return result;
	}
	
	
	public List<String> selectColumn() {
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<String> strarr = new ArrayList<String>();
		
		String sql = "select column_name from information_schema.columns where table_schema='test' and table_name='" + tablename + "'";

		try {
			conn = new DBConnection().getConnection();
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
	
			while(rs.next()) {
				strarr.add(rs.getString(1));
			}
		
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {try { rs.close(); } catch (SQLException e) {}}
			if (pstmt != null) {try { pstmt.close(); } catch (SQLException e) {}}
			if (conn != null) {try { conn.close(); } catch (SQLException e) {}}
		}
		
		return strarr;
	}

	public void clearDB() {
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		String sql = "delete from DOC1";

		try {
			conn = new DBConnection().getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {try { pstmt.close(); } catch (SQLException e) {}}
			if (conn != null) {try { conn.close(); } catch (SQLException e) {}}
		}
		
	}

	public int countData() {

		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int result = 0;
		
		String sql = "select count(*) from DOC1";

		try {
			conn = new DBConnection().getConnection();
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			rs.next();
			result = rs.getInt(1);
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {try { rs.close(); } catch (SQLException e) {}}
			if (pstmt != null) {try { pstmt.close(); } catch (SQLException e) {}}
			if (conn != null) {try { conn.close(); } catch (SQLException e) {}}
		}
		return result;
	}
}
