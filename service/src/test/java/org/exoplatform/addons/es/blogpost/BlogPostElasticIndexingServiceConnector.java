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
import org.exoplatform.addons.es.index.elastic.ElasticIndexingServiceConnector;
import org.exoplatform.container.xml.InitParams;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by The eXo Platform SAS
 * Author : Thibault Clement
 * tclement@exoplatform.com
 * 8/18/15
 */
public class BlogPostElasticIndexingServiceConnector extends ElasticIndexingServiceConnector {

  public final static String PREFIX_ID = "blog_post_";

  BlogpostService blogpostService;

  public BlogPostElasticIndexingServiceConnector(InitParams initParams) {
    super(initParams);
    blogpostService = new BlogpostService();
  }

  public BlogPostElasticIndexingServiceConnector(InitParams initParams, BlogpostService blogpostService) {
    super(initParams);
    this.blogpostService = blogpostService;
  }

  @Override
  public Document create(String id) {
    return toDocument(blogpostService.findBlogpost(Long.parseLong(id)));
  }

  @Override
  public Document update(String id) {
    return toDocument(blogpostService.findBlogpost(Long.parseLong(id)));
  }

  @Override
  public String delete(String id) {
    return String.valueOf(blogpostService.findBlogpost(Long.parseLong(id)).getId());
  }

  private Document toDocument(Blogpost blogpost) {
    Document postDocument = new Document();
    postDocument.setId(PREFIX_ID+blogpost.getId());
    postDocument.setCreatedDate(blogpost.getPostDate());
    postDocument.setType("blog");
    postDocument.setUrl("");
    Map<String, String> fields = new HashMap<>();
    fields.put("title", blogpost.getTitle());
    fields.put("content", blogpost.getContent());
    fields.put("author", blogpost.getAuthor());
    postDocument.setFields(fields);
    String[] permissions = {"admin", "jedi", "null"};
    postDocument.setPermissions(permissions);
    return postDocument;
  }

}

