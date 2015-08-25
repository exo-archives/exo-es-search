/* 
* Copyright (C) 2003-2015 eXo Platform SAS.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see http://www.gnu.org/licenses/ .
*/
package org.exoplatform.addons.es.blogpost;

import java.util.*;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 8/21/15
 */
public class BlogpostService {

  Map<Long, Blogpost> blogposts = new HashMap<>();

  public Blogpost findBlogpost(Long id) {
    return blogposts.get(id);
  }

  public List<Blogpost> findAll() {
    List<Blogpost>  blogpostList = new ArrayList<>();
    for (Long postId : blogposts.keySet()) {
      blogpostList.add(blogposts.get(postId));
    }
    return blogpostList;
  }

  public void create(Blogpost blogpost) {
    blogposts.put(blogpost.getId(), blogpost);
  }

  public void initData() {
    Blogpost deathStartProject = new Blogpost();
    deathStartProject.setId(1L);
    deathStartProject.setAuthor("Thibault");
    deathStartProject.setPostDate(new Date());
    deathStartProject.setTitle("Death Star Project");
    deathStartProject.setContent("The new Death Star Project is under the responsability of the Vador Team : Benoit," +
        " Thibault and Tuyen");

    Blogpost plf43 = new Blogpost();
    plf43.setId(2L);
    plf43.setAuthor("Patrice");
    plf43.setPostDate(new Date());
    plf43.setTitle("Platform 4.3 Feature Scope");
    plf43.setContent("While for eXo Platform 4.2 we wanted to help you to boost user engagement, " +
        "eXo Platform 4.3 will help your users to get more work done!\n" +
        "We have planned a release for the very end of the year. So let’s see what we have in the oven");

    Blogpost coolDoc = new Blogpost();
    coolDoc.setId(3L);
    coolDoc.setAuthor("Giang Tran");
    coolDoc.setPostDate(new Date());
    coolDoc.setTitle("Cool documentation updates");
    coolDoc.setContent("Among the big number of handy topics on eXo Documentation Website in last July, we in " +
        "the Doc Team would like to summarise the major updates to the eXo Documentation Website in this blog. " +
        "Let’s see what they are.");

    blogposts.put(deathStartProject.getId(), deathStartProject);
    blogposts.put(plf43.getId(), plf43);
    blogposts.put(coolDoc.getId(), coolDoc);

  }

}


