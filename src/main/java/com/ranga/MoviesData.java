/**
 * 
 */
package com.ranga;

import java.io.Serializable;

/**
 * @author Ranga_Reddy1
 *
 */
public class MoviesData implements Serializable {
	
	private static final long serialVersionUID = -907861392316951392L;
	
	private Integer year;
	private Long length;
	private String title;
	private String subject;
	private String actor;
	private String actress;
	private String director;
	private Integer popularity;
	private String award;
	
	public MoviesData(Integer year, Long length, String title, String subject, String actor, String actress,
			String director, Integer popularity, String award) {
		super();
		this.year = year;
		this.length = length;
		this.title = title;
		this.subject = subject;
		this.actor = actor;
		this.actress = actress;
		this.director = director;
		this.popularity = popularity;
		this.award = award;
	}
	
	public Integer getYear() {
		return year;
	}
	public void setYear(Integer year) {
		this.year = year;
	}
	public Long getLength() {
		return length;
	}
	public void setLength(Long length) {
		this.length = length;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getSubject() {
		return subject;
	}
	public void setSubject(String subject) {
		this.subject = subject;
	}
	public String getActor() {
		return actor;
	}
	public void setActor(String actor) {
		this.actor = actor;
	}
	public String getActress() {
		return actress;
	}
	public void setActress(String actress) {
		this.actress = actress;
	}
	public String getDirector() {
		return director;
	}
	public void setDirector(String director) {
		this.director = director;
	}
	public Integer getPopularity() {
		return popularity;
	}
	public void setPopularity(Integer popularity) {
		this.popularity = popularity;
	}
	public String getAward() {
		return award;
	}
	public void setAward(String award) {
		this.award = award;
	}
	
	@Override
	public String toString() {
		return "MoviesData [year=" + year + ", length=" + length + ", title=" + title + ", subject=" + subject
				+ ", actor=" + actor + ", actress=" + actress + ", director=" + director + ", popularity=" + popularity
				+ ", award=" + award + "]";
	}
}
