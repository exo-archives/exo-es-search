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

import org.exoplatform.addons.es.domain.Document;
import org.exoplatform.addons.es.index.IndexingServiceConnector;
import org.exoplatform.container.xml.InitParams;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 8/18/15
 */
public class BlogPostIndexingServiceConnector extends IndexingServiceConnector {

  public final static String PREFIX_ID = "blog_post_";

  BlogpostService blogpostService;

  public BlogPostIndexingServiceConnector(InitParams initParams) {
    super(initParams);
    blogpostService = new BlogpostService();
  }

  @Override
  public Document index(String id) {
    return toDocument(blogpostService.findBlogpost(Long.parseLong(id)));
  }

  @Override
  public String delete(String id) {
    return String.valueOf(blogpostService.findBlogpost(Long.parseLong(id)).getId());
  }

  @Override
  public List<String> deleteAll() {
    List<String> postIds = new ArrayList<>();
    List<Blogpost> blogposts = blogpostService.findAll();
    for (Blogpost blogpost: blogposts) {
      postIds.add(PREFIX_ID+String.valueOf(blogpost.getId()));
    }
    return postIds;
  }

  private Document toDocument(Blogpost blogpost) {
    Document postDocument = new Document();
    //TODO transform blogpost object to document object
    return postDocument;
  }

}

